import numpy as np
from affine import Affine

import dagster as dg
from heat_islands_workflow.assets.raster.common import fetch_raster
from heat_islands_workflow.partitions import state_partitions, zone_partitions


def world_cover_factory(
    suffix: str, partitions_def: dg.PartitionsDefinition,
) -> dg.AssetsDefinition:
    @dg.asset(
        name="world_cover",
        key_prefix=f"raster_{suffix}",
        ins={"bounds": dg.AssetIn(["bounds", suffix])},
        partitions_def=partitions_def,
        io_manager_key="raster_io_manager",
        group_name=f"cover_raster_{suffix}",
    )
    def _asset(bounds: dict[str, float]) -> tuple[np.ndarray, str, Affine]:
        return fetch_raster(
            "http://localhost:8000/worldcover/raster",
            params=dict(
                xmin=bounds["xmin"],
                ymin=bounds["ymin"],
                xmax=bounds["xmax"],
                ymax=bounds["ymax"],
            ),
        )

    return _asset


dassets = [
    world_cover_factory("zone", zone_partitions),
    world_cover_factory("state", state_partitions),
]
