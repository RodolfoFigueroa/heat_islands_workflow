from collections.abc import Sequence
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio as rio
import rasterio.mask  # pylint: disable=unused-import
import shapely
from rasterio import DatasetBase

import dagster as dg
from heat_islands_workflow.partitions import zone_partitions
from heat_islands_workflow.resources import PathResource


def overlay_geometries(
    ds: DatasetBase, geometries: Sequence[shapely.Geometry],
) -> list[float]:
    temps = []
    for geom in geometries:
        masked, _ = rio.mask.mask(ds, [geom], crop=True, nodata=-99999)
        masked[masked == -99999] = np.nan
        temps.append(np.nanmean(masked))
    return temps


@dg.asset(
    ins={
        "raster_path": dg.AssetIn(
            ["raster_zone", "suhi"], input_manager_key="raster_path_io_manager",
        ),
    },
    partitions_def=zone_partitions,
    io_manager_key="gpkg_io_manager",
)
def polygons_with_temp(
    context: dg.AssetExecutionContext, path_resource: PathResource, raster_path: Path,
) -> gpd.GeoDataFrame:
    fpath = (
        Path(path_resource.population_grids_path)
        / f"final/zone_agebs/shaped/2020/{context.partition_key}.gpkg"
    )
    df = gpd.read_file(fpath)

    with rio.open(raster_path) as ds:
        df = df.to_crs(ds.crs)
        df["temp"] = overlay_geometries(ds, df["geometry"])
    return df
