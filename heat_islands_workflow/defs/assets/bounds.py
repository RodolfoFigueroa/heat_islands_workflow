from pathlib import Path

import ee
import geopandas as gpd
import shapely

import dagster as dg
from heat_islands_workflow.defs.partitions import state_partitions, zone_partitions
from heat_islands_workflow.defs.resources import PathResource, PostGISResource


def get_metropolitan_zone_bounds(
    zone: str,
    postgis_resource: PostGISResource,
) -> gpd.GeoDataFrame:
    with postgis_resource.connect() as conn:
        return gpd.GeoDataFrame.from_postgis(
            """
            SELECT census_2020_ageb.geometry, census_2020_ageb."CVEGEO"
                FROM census_2020_ageb
            INNER JOIN census_2020_mun
                ON census_2020_ageb."CVE_MUN" = census_2020_mun."CVEGEO"
            INNER JOIN metropoli_2020
                ON census_2020_mun."CVE_MET" = metropoli_2020."CVE_MET"
            WHERE metropoli_2020."CVE_MET" = %(met_zone)s
            """,
            conn,
            geom_col="geometry",
            crs="EPSG:6372",
            params={"met_zone": zone},
        )


def get_fua_bounds(zone: str, path_resource: PathResource) -> gpd.GeoDataFrame:
    iso, city = zone.split("+")

    fpath = (
        Path(path_resource.ghsl_path)
        / "GHS_FUA_UCDB2015_GLOBE_R2019A_54009_1K_V1_0.gpkg"
    )
    df = gpd.read_file(fpath).query(f"(Cntry_ISO == '{iso}') & (eFUA_name == '{city}')")
    return gpd.GeoDataFrame(df)


def buffer_bounds(
    df: gpd.GeoDataFrame,
    buffer_size: float = 10_000,
) -> gpd.GeoDataFrame:
    utm = df.estimate_utm_crs()
    df = df.to_crs(utm)

    xmin, ymin, xmax, ymax = df.total_bounds
    xmin -= buffer_size
    ymin -= buffer_size
    xmax += buffer_size
    ymax += buffer_size

    return gpd.GeoDataFrame(
        geometry=[shapely.box(xmin, ymin, xmax, ymax)],
        crs=df.crs,
    ).to_crs("EPSG:4326")


def find_largest_blob(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    merged: shapely.geometry.Polygon | shapely.geometry.MultiPolygon = df.assign(
        geometry=lambda df: df["geometry"].buffer(50)
    )["geometry"].unary_union
    if isinstance(merged, shapely.geometry.Polygon):
        return gpd.GeoDataFrame(geometry=[merged], crs=df.crs)

    max_area, max_geom = 0, None
    for geom in merged.geoms:
        area = geom.area
        if area > max_area:
            max_area = area
            max_geom = geom

    if max_geom is None:
        err = "No geometries found in merged result"
        raise ValueError(err)

    return gpd.GeoDataFrame(geometry=[max_geom], crs=df.crs)


@dg.asset(
    key=["bounds_zone", "shapely"],
    partitions_def=zone_partitions,
    io_manager_key="gpkg_io_manager",
    group_name="bounds",
)
def bounds_shapely(
    context: dg.AssetExecutionContext,
    postgis_resource: PostGISResource,
    path_resource: PathResource,
) -> gpd.GeoDataFrame:
    if "+" in context.partition_key:
        out = get_fua_bounds(context.partition_key, path_resource)
    else:
        out = get_metropolitan_zone_bounds(context.partition_key, postgis_resource)

    return (
        find_largest_blob(out)
        .assign(geometry=lambda df: df["geometry"].buffer(1000))
        .to_crs("EPSG:4326")
    )


@dg.asset(
    key=["bounds_zone", "ee"],
    ins={"bounds": dg.AssetIn(["bounds_zone", "shapely"])},
    partitions_def=zone_partitions,
    io_manager_key="earthengine_io_manager",
    group_name="bounds",
)
def bounds_ee(bounds: gpd.GeoDataFrame) -> ee.geometry.Geometry:
    bbox = bounds["geometry"].item()
    return ee.geometry.Geometry.Polygon(
        list(zip(*bbox.exterior.coords.xy, strict=False)),
    )


@dg.asset(
    key=["bounds", "state"],
    partitions_def=state_partitions,
    io_manager_key="json_io_manager",
)
def state_bounds(
    context: dg.AssetExecutionContext,
    path_resource: PathResource,
) -> dict[str, float]:
    fpath = (
        Path(path_resource.population_grids_path)
        / "final"
        / "framework"
        / "states"
        / "2020.gpkg"
    )

    code = int(context.partition_key)  # noqa: F841
    df = gpd.GeoDataFrame(
        gpd.read_file(fpath).to_crs("EPSG:6372").query("CVE_ENT == @code"),
    )
    xmin, ymin, xmax, ymax = buffer_bounds(df, buffer_size=10_000)
    return {"xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax}
