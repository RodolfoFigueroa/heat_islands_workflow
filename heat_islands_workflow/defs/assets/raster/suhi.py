import ee
import numpy as np
from affine import Affine

import dagster as dg
from heat_islands_workflow.defs.assets.common import download_ee_raster, fetch_raster
from heat_islands_workflow.defs.partitions import (
    month_partitions,
    year_partitions,
    zone_partitions,
)

partition_mapping = dg.MultiPartitionMapping(
    {
        "area": dg.DimensionPartitionMapping(
            dimension_name="area",
            partition_mapping=dg.IdentityPartitionMapping(),
        ),
        "year_and_season": dg.DimensionPartitionMapping(
            dimension_name="year",
            partition_mapping=dg.StaticPartitionMapping(
                {
                    "2024_winter": "2024",
                    "2024_spring": "2024",
                    "2024_summer": "2024",
                    "2024_autumn": "2024",
                    "2025_winter": "2025",
                    "2025_spring": "2025",
                    "2025_summer": "2025",
                    "2025_autumn": "2025",
                },
            ),
        ),
    },
)


@dg.asset(
    key=["raster_zone_download", "suhi"],
    ins={
        "bounds": dg.AssetIn(["bounds_zone", "ee"]),
        "region_temp_map": dg.AssetIn(
            ["raster_zone", "region_temp"],
            partition_mapping=partition_mapping,
        ),
        "temp_img_map": dg.AssetIn(
            ["raster_zone", "temp_seasonal"],
            partition_mapping=partition_mapping,
        ),
    },
    partitions_def=dg.MultiPartitionsDefinition(
        {"area": zone_partitions, "year": year_partitions},
    ),
    io_manager_key="raster_io_manager",
    group_name="downloaded_raster_zone",
)
def raster_suhi(
    context: dg.AssetExecutionContext,
    bounds: ee.geometry.Geometry,
    region_temp_map: dict[str, dict[str, float]],
    temp_img_map: dict[str, ee.image.Image],
) -> tuple[np.ndarray, str, Affine, float]:
    _, year = context.partition_key.split("|")

    images = []
    for key in region_temp_map:
        if year not in key:
            continue

        rural_temp = region_temp_map[key]["rural"]
        temp_img = temp_img_map[key]
        suhi_img = temp_img.subtract(ee.Number(rural_temp)).float()
        images.append(suhi_img)

    out_img = ee.imagecollection.ImageCollection.fromImages(images).mean()
    return download_ee_raster(out_img, nodata=-9999, bounds=bounds)


@dg.asset(
    key=["raster_zone_download", "suhi_categorical"],
    ins={"raster_suhi": dg.AssetIn(["raster_zone_download", "suhi"])},
    partitions_def=dg.MultiPartitionsDefinition(
        {"area": zone_partitions, "year": year_partitions},
    ),
    io_manager_key="raster_io_manager",
    group_name="downloaded_raster_zone",
)
def raster_suhi_categorical(
    raster_suhi: tuple[np.ndarray, str, Affine],
) -> tuple[np.ndarray, str, Affine, int]:
    data, crs, transform = raster_suhi

    mu = np.nanmean(data)
    sigma = np.nanstd(data)

    bins = np.array(
        [
            -np.inf,
            mu - 2.5 * sigma,
            mu - 1.5 * sigma,
            mu - 0.5 * sigma,
            mu + 0.5 * sigma,
            mu + 1.5 * sigma,
            mu + 2.5 * sigma,
            np.inf,
        ],
    )

    digitized = np.digitize(data, bins).astype(np.int8)
    digitized[digitized == 0] = -95  # after shifting it will be -99 (nodata)
    digitized[digitized == len(bins)] = -95
    digitized = digitized - 4

    return digitized, crs, transform, -99


@dg.asset(
    key=["raster_zone", "suhi_monthly"],
    ins={"bounds": dg.AssetIn(["bounds_zone", "ee"])},
    partitions_def=dg.MultiPartitionsDefinition(
        {"city": zone_partitions, "month": month_partitions},
    ),
    io_manager_key="raster_io_manager",
    group_name="temp_raster_zone",
)
def raster_suhi_monthly(
    context: dg.AssetExecutionContext,
    bounds: dict[str, float],
) -> tuple[np.ndarray, str, Affine]:
    _, month = context.partition_key.split("|")

    data, crs, transform = fetch_raster(
        "http://localhost:8000/suhi/raster/monthly/suhi",
        params={
            "xmin": bounds["xmin"],
            "ymin": bounds["ymin"],
            "xmax": bounds["xmax"],
            "ymax": bounds["ymax"],
            "year": 2024,
            "month": month,
        },
    )
    data = data.astype(float)
    return data, crs, transform
