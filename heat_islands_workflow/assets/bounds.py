from pathlib import Path

import geopandas as gpd

import dagster as dg
from heat_islands_workflow.assets.common import buffer_bounds
from heat_islands_workflow.partitions import state_partitions, zone_partitions
from heat_islands_workflow.resources import PathResource


@dg.asset(
    name="zone",
    key_prefix="bounds",
    partitions_def=zone_partitions,
    io_manager_key="json_io_manager",
)
def bounds(
    context: dg.AssetExecutionContext, path_resource: PathResource,
) -> dict[str, float]:
    fpath = (
        Path(path_resource.population_grids_path)
        / f"final/reprojected/merged/{context.partition_key}.gpkg"
    )
    df = gpd.read_file(fpath)
    xmin, ymin, xmax, ymax = buffer_bounds(df, buffer_size=10_000)
    return dict(xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax)


@dg.asset(
    name="state",
    key_prefix="bounds",
    partitions_def=state_partitions,
    io_manager_key="json_io_manager",
)
def state_bounds(
    context: dg.AssetExecutionContext, path_resource: PathResource,
) -> dict[str, float]:
    fpath = (
        Path(path_resource.population_grids_path) / "final/framework/states/2020.gpkg"
    )

    code = int(context.partition_key)  # pylint: disable=unused-variable
    df = gpd.read_file(fpath).to_crs("EPSG:6372").query("CVE_ENT == @code")
    xmin, ymin, xmax, ymax = buffer_bounds(df, buffer_size=10_000)
    return dict(xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax)
