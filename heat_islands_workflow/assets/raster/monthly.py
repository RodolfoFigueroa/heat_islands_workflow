import numpy as np
from affine import Affine

import dagster as dg
from heat_islands_workflow.assets.raster.common import fetch_raster
from heat_islands_workflow.partitions import (
    month_partitions,
    state_partitions,
    year_and_month_partitions,
    zone_partitions,
)


def raster_temp_monthly_factory(
    suffix: str, partitions_def: dg.PartitionsDefinition,
) -> dg.AssetsDefinition:
    @dg.asset(
        name="temp_monthly",
        key_prefix=f"raster_{suffix}",
        ins={"bounds": dg.AssetIn(["bounds", suffix])},
        partitions_def=dg.MultiPartitionsDefinition(
            {"area": partitions_def, "year_and_month": year_and_month_partitions},
        ),
        io_manager_key="raster_io_manager",
        group_name=f"temp_raster_{suffix}",
    )
    def _asset(
        context: dg.AssetExecutionContext, bounds: dict[str, float],
    ) -> tuple[np.ndarray, str, Affine]:
        _, year_and_month = context.partition_key.split("|")
        year, month = year_and_month.split("_")

        data, crs, transform = fetch_raster(
            "http://localhost:8000/suhi/raster/monthly/temp",
            params=dict(
                xmin=bounds["xmin"],
                ymin=bounds["ymin"],
                xmax=bounds["xmax"],
                ymax=bounds["ymax"],
                year=year,
                month=month,
            ),
        )
        data = data.astype(float)
        return data, crs, transform

    return _asset


dassets = [
    raster_temp_monthly_factory("zone", zone_partitions),
    raster_temp_monthly_factory("state", state_partitions),
]
