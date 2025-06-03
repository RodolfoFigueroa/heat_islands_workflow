from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
import shapely
from scipy.interpolate import make_smoothing_spline

import dagster as dg
from heat_islands_workflow.partitions import zone_partitions
from heat_islands_workflow.resources import PathResource


def generate_circles(
    center: tuple[float, float], circle_radius: float, max_radius: float,
) -> tuple[np.ndarray, list[shapely.geometry.Polygon]]:
    radii = np.arange(circle_radius, max_radius + circle_radius, circle_radius)
    circles = [
        shapely.geometry.Point(center).buffer(radius, resolution=32) for radius in radii
    ]
    return radii, circles


def get_trim_idx(
    df_pop: gpd.GeoDataFrame,
    centroid: tuple[float, float],
    circle_radius: float,
    max_radius: float,
) -> int:
    _, circles = generate_circles(centroid, circle_radius, max_radius)
    circles = (
        gpd.GeoSeries(circles, crs="ESRI:54009")
        .to_frame()
        .to_crs(df_pop.crs)
        .reset_index(names="circle_idx")
    )

    overlay = df_pop.overlay(circles)
    return (
        (overlay.groupby("circle_idx")["POBTOT"].sum() / df_pop["POBTOT"].sum()) >= 0.95
    ).idxmax()


@dg.asset(
    ins={"df": dg.AssetIn("polygons_with_temp")},
    partitions_def=zone_partitions,
    io_manager_key="csv_io_manager",
)
def pop_exposed(df: gpd.GeoDataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["temp", "POBTOT"])
    pop = df.sort_values("temp").set_index("temp")["POBTOT"]
    total_pop = pop.sum()

    x = pop.index
    cdf = pop.cumsum() / total_pop

    spline = make_smoothing_spline(x, cdf, lam=0.05)
    pdf = spline.derivative()(x)

    return (
        pd.DataFrame(zip(x, pdf, cdf, strict=False), columns=["temp", "pdf", "cdf"])
        .set_index("temp")
        .assign(
            pdf_abs=lambda df: df["pdf"] * total_pop,
            cdf_abs=lambda df: df["cdf"] * total_pop,
        )
    )


@dg.asset(
    ins={
        "df_pop": dg.AssetIn("polygons_with_temp"),
        "bounds": dg.AssetIn(["bounds", "zone"]),
    },
    partitions_def=zone_partitions,
    io_manager_key="csv_io_manager",
)
def radial_distribution(
    context: dg.AssetExecutionContext,
    path_resource: PathResource,
    df_pop: gpd.GeoDataFrame,
    bounds: dict[str, float],
) -> pd.DataFrame:
    centroid_path = Path(path_resource.centroid_path)

    centroid_orig = gpd.read_file(centroid_path / f"{context.partition_key}.gpkg")
    centroid = centroid_orig.to_crs("EPSG:4326")["geometry"].item()
    centroid_mollweide = centroid_orig.to_crs("ESRI:54009")["geometry"].item()

    response = requests.get(
        "http://localhost:8000/suhi/data/radial",
        params=dict(
            xmin=bounds["xmin"],
            ymin=bounds["ymin"],
            xmax=bounds["xmax"],
            ymax=bounds["ymax"],
            year=2024,
            season="Qall",
            x=centroid.x,
            y=centroid.y,
        ),
        timeout=500,
    )

    response_json = response.json()
    radii = response_json["radii"]
    pdf = response_json["cdf"]

    trim_idx = get_trim_idx(
        df_pop,
        (centroid_mollweide.x, centroid_mollweide.y),
        radii[1] - radii[0],
        radii[-1],
    )

    radii_trimmed = radii[:trim_idx]
    pdf_trimmed = pdf[:trim_idx]

    spline = make_smoothing_spline(radii_trimmed, pdf_trimmed, lam=0.1)

    x = np.arange(radii_trimmed[0], radii_trimmed[-1], 100)
    y = spline(x)

    return pd.DataFrame(zip(x, y, strict=False), columns=["radius", "pdf"]).set_index("radius")
