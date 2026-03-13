import ee
import numpy as np
from affine import Affine

import dagster as dg
from heat_islands_workflow.defs.assets.common import (
    download_ee_raster,
    get_date_range,
    get_season_date_range,
)
from heat_islands_workflow.defs.partitions import (
    state_partitions,
    year_and_month_partitions,
    year_and_season_partitions,
    zone_partitions,
)


def fmask(image: ee.image.Image) -> ee.image.Image:
    """Calculates the cloud mask for a Landsat image.

    Parameters
    ----------
    image : ee.Image
        The image to analyze. Must have valid cloud bands.

    Returns
    -------
    ee.Image
        The resultant cloud mask image with binary values. A 0 indicates that a cloud was present.
    """

    qa = image.select("QA_PIXEL")

    dilated_cloud_bit = 1
    cloud_bit = 3
    cloud_shadow_bit = 4

    mask = qa.bitwiseAnd(1 << dilated_cloud_bit).eq(0)
    mask = mask.And(qa.bitwiseAnd(1 << cloud_bit).eq(0))
    mask = mask.And(qa.bitwiseAnd(1 << cloud_shadow_bit).eq(0))

    return image.updateMask(mask)


def image_collection_to_image(
    bounds: ee.geometry.Geometry,
    start_date: str,
    end_date: str,
) -> ee.image.Image:
    filtered: ee.imagecollection.ImageCollection = (
        ee.imagecollection.ImageCollection("LANDSAT/LC09/C02/T1_L2")
        .filterDate(start_date, end_date)
        .filterBounds(bounds)
    )

    if filtered.size().getInfo() == 0:
        err = "No measurements for given date and location found."
        raise ValueError(err)

    return (
        filtered.map(fmask)
        .select("ST_B10")
        .mean()
        .multiply(0.00341802)
        .add(149 - 273.15)
        .clip(bounds)
    )


def raster_temp_monthly_factory(
    suffix: str,
    partitions_def: dg.PartitionsDefinition,
) -> dg.AssetsDefinition:
    @dg.asset(
        key=[f"raster_{suffix}", "temp_monthly"],
        ins={"bounds": dg.AssetIn(["bounds_zone", "ee"])},
        partitions_def=dg.MultiPartitionsDefinition(
            {"area": partitions_def, "year_and_month": year_and_month_partitions},
        ),
        io_manager_key="earthengine_io_manager",
        group_name=f"temp_raster_{suffix}",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        bounds: ee.geometry.Geometry,
    ) -> ee.image.Image:
        _, year_and_month = context.partition_key.split("|")
        year, month = year_and_month.split("_")
        start_date, end_date = get_date_range(int(month), int(year))
        return image_collection_to_image(bounds, start_date, end_date)

    return _asset


def raster_temp_seasonal_factory(
    suffix: str,
    partitions_def: dg.PartitionsDefinition,
) -> dg.AssetsDefinition:
    @dg.asset(
        key=[f"raster_{suffix}", "temp_seasonal"],
        ins={"bounds": dg.AssetIn(["bounds_zone", "ee"])},
        partitions_def=dg.MultiPartitionsDefinition(
            {"area": partitions_def, "year_and_season": year_and_season_partitions},
        ),
        io_manager_key="earthengine_io_manager",
        group_name=f"temp_raster_{suffix}",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        bounds: ee.geometry.Geometry,
    ) -> ee.image.Image:
        _, year_and_season = context.partition_key.split("|")
        year, season = year_and_season.split("_")
        start_date, end_date = get_season_date_range(season, int(year))
        return image_collection_to_image(bounds, start_date, end_date)

    return _asset


def raster_temp_seasonal_download_factory(
    suffix: str,
    partitions_def: dg.PartitionsDefinition,
) -> dg.AssetsDefinition:
    @dg.asset(
        key=[f"raster_{suffix}_download", "temp_seasonal_download"],
        ins={
            "bounds": dg.AssetIn(["bounds_zone", "ee"]),
            "img": dg.AssetIn([f"raster_{suffix}", "temp_seasonal"]),
        },
        partitions_def=dg.MultiPartitionsDefinition(
            {"area": partitions_def, "year_and_season": year_and_season_partitions},
        ),
        io_manager_key="raster_io_manager",
        group_name=f"downloaded_raster_{suffix}",
    )
    def _asset(
        bounds: ee.geometry.Geometry,
        img: ee.image.Image,
    ) -> tuple[np.ndarray, str, Affine, float]:
        return download_ee_raster(img, nodata=-9999, bounds=bounds)

    return _asset


dassets = [
    factory(suffix, partitions)
    for suffix, partitions in zip(
        ["zone", "state"],
        [zone_partitions, state_partitions],
        strict=True,
    )
    for factory in [
        raster_temp_monthly_factory,
        raster_temp_seasonal_factory,
        raster_temp_seasonal_download_factory,
    ]
]
