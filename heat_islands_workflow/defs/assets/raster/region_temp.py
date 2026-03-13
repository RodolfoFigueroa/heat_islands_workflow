import ee

import dagster as dg
from heat_islands_workflow.defs.partitions import (
    year_and_season_partitions,
    zone_partitions,
)


@dg.op
def get_region_temperature(
    temp_img: ee.image.Image,
    mask: ee.image.Image,
    bounds: ee.geometry.Geometry,
) -> float:
    res = (
        temp_img.updateMask(mask)
        .reduceRegion(
            ee.reducer.Reducer.mean(),
            geometry=bounds,
            scale=100,
        )
        .getInfo()
    )

    if res is None:
        err = "No data found for the given bounds and masks."
        raise ValueError(err)

    temp = res["ST_B10"]

    if not isinstance(temp, float):
        err = f"Expected a float, got {type(temp)}"
        raise TypeError(err)

    return temp


@dg.op(out=dg.Out(io_manager_key="json_io_manager"))
def aggregate_temps(urban_temp: float, rural_temp: float) -> dict[str, float]:
    return {"urban": urban_temp, "rural": rural_temp}


@dg.graph_asset(
    key=["raster_zone", "region_temp"],
    ins={
        "bounds": dg.AssetIn(["bounds_zone", "ee"]),
        "urban_mask": dg.AssetIn(["raster_zone", "worldcover", "urban_mask"]),
        "rural_mask": dg.AssetIn(["raster_zone", "worldcover", "rural_mask"]),
        "temp_seasonal": dg.AssetIn(["raster_zone", "temp_seasonal"]),
    },
    partitions_def=dg.MultiPartitionsDefinition(
        {"area": zone_partitions, "year_and_season": year_and_season_partitions},
    ),
    group_name="temp_raster_zone",
)
def rural_temp(
    bounds: ee.geometry.Geometry,
    urban_mask: ee.image.Image,
    rural_mask: ee.image.Image,
    temp_seasonal: ee.image.Image,
) -> dict[str, float]:
    rural_temp = get_region_temperature(temp_seasonal, rural_mask, bounds)
    urban_temp = get_region_temperature(temp_seasonal, urban_mask, bounds)
    return aggregate_temps(urban_temp, rural_temp)
