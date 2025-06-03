import numpy as np
from affine import Affine

import dagster as dg
from heat_islands_workflow.assets.raster.common import fetch_raster
from heat_islands_workflow.partitions import month_partitions, zone_partitions


@dg.asset(
    name="suhi",
    key_prefix="raster_zone",
    ins={"bounds": dg.AssetIn(["bounds", "zone"])},
    partitions_def=zone_partitions,
    io_manager_key="raster_io_manager",
    group_name="temp_raster_zone",
)
def raster_suhi(bounds: dict[str, float]) -> tuple[np.ndarray, str, Affine]:
    return fetch_raster(
        "http://localhost:8000/suhi/raster/suhi",
        dict(
            xmin=bounds["xmin"],
            ymin=bounds["ymin"],
            xmax=bounds["xmax"],
            ymax=bounds["ymax"],
            year=2024,
            season="Qall",
        ),
    )


@dg.asset(
    name="suhi_monthly",
    key_prefix="raster_zone",
    ins={"bounds": dg.AssetIn(["bounds", "zone"])},
    partitions_def=dg.MultiPartitionsDefinition(
        {"city": zone_partitions, "month": month_partitions},
    ),
    io_manager_key="raster_io_manager",
    group_name="temp_raster_zone",
)
def raster_suhi_monthly(
    context: dg.AssetExecutionContext, bounds: dict[str, float],
) -> tuple[np.ndarray, str, Affine]:
    _, month = context.partition_key.split("|")

    data, crs, transform = fetch_raster(
        "http://localhost:8000/suhi/raster/monthly/suhi",
        params=dict(
            xmin=bounds["xmin"],
            ymin=bounds["ymin"],
            xmax=bounds["xmax"],
            ymax=bounds["ymax"],
            year=2024,
            month=month,
        ),
    )
    data = data.astype(float)
    return data, crs, transform
