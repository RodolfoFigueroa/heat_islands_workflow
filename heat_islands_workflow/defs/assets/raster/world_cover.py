import ee

import dagster as dg
from heat_islands_workflow.defs.partitions import zone_partitions


@dg.asset(
    key=["raster_zone", "worldcover"],
    ins={"bounds": dg.AssetIn(["bounds_zone", "ee"])},
    partitions_def=zone_partitions,
    io_manager_key="earthengine_io_manager",
    group_name="worldcover_raster_zone",
)
def raster_worldcover(bounds: ee.geometry.Geometry) -> ee.image.Image:
    return ee.imagecollection.ImageCollection("ESA/WorldCover/v200").mode().clip(bounds)


@dg.asset(
    key=["raster_zone", "worldcover", "urban_mask"],
    ins={"worldcover_img": dg.AssetIn(["raster_zone", "worldcover"])},
    partitions_def=zone_partitions,
    io_manager_key="earthengine_io_manager",
    group_name="worldcover_raster_zone",
)
def urban_mask_raster(worldcover_img: ee.image.Image) -> ee.image.Image:
    return worldcover_img.eq(ee.Number(50))


@dg.asset(
    key=["raster_zone", "worldcover", "unwanted_mask"],
    ins={"worldcover_img": dg.AssetIn(["raster_zone", "worldcover"])},
    partitions_def=zone_partitions,
    io_manager_key="earthengine_io_manager",
    group_name="worldcover_raster_zone",
)
def unwanted_mask_raster(worldcover_img: ee.image.Image) -> ee.image.Image:
    snow_mask = worldcover_img.neq(ee.Number(70))
    water_mask = worldcover_img.neq(ee.Number(80))
    return snow_mask.And(water_mask)


@dg.asset(
    key=["raster_zone", "worldcover", "rural_mask"],
    ins={
        "urban_mask": dg.AssetIn(["raster_zone", "worldcover", "urban_mask"]),
        "unwanted_mask": dg.AssetIn(["raster_zone", "worldcover", "unwanted_mask"]),
    },
    partitions_def=zone_partitions,
    io_manager_key="earthengine_io_manager",
    group_name="worldcover_raster_zone",
)
def rural_mask_raster(
    urban_mask: ee.image.Image,
    unwanted_mask: ee.image.Image,
) -> ee.image.Image:
    return (
        urban_mask.focalMax(radius=500, units="meters", kernelType="circle")
        .bitwiseNot()
        .bitwiseAnd(unwanted_mask)
    )
