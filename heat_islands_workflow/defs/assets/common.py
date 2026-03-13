import calendar
import tempfile
from pathlib import Path

import ee
import geemap
import numpy as np
import rasterio as rio
import requests
import shapely
from affine import Affine


def bbox_to_ee(bbox: shapely.Polygon) -> ee.geometry.Geometry:
    """Converts a shapely polygon to an EarthEngine geometry.

    Parameters
    ----------
    bbox: shapely.Polygon
        Polygon to convert.

    Returns
    -------
    ee.Geometry.Polygon
        Equivalent EarthEngine geometry.
    """
    return ee.geometry.Geometry.Polygon(
        list(zip(*bbox.exterior.coords.xy, strict=False)),
    )


def bounds_to_ee(
    xmin: float,
    ymin: float,
    xmax: float,
    ymax: float,
) -> ee.geometry.Geometry:
    """Converts a list of bounds to an EarthEngine geometry.

    Parameters
    ----------
    xmin: float
        Minimum x coordinate.

    ymin: float
        Minimum y coordinate.

    xmax: float
        Maximum x coordinate.

    ymax: float
        Maximum y coordinate.


    Returns
    -------
    ee.Geometry.Polygon
        Equivalent EarthEngine geometry.
    """
    return bbox_to_ee(shapely.box(xmin, ymin, xmax, ymax))


def fetch_raster(url: str, params: dict) -> tuple[np.ndarray, str, Affine]:
    response = requests.get(url, params=params, timeout=500)

    if response.status_code != 200:
        err = f"Error fetching raster data: {response.status_code} - {response.text}"
        raise ValueError(err)

    response_json = response.json()

    data = np.reshape(
        response_json["data"],
        (response_json["height"], response_json["width"]),
    )
    crs = response_json["crs"]
    transform = Affine(*response_json["transform"])

    return data, crs, transform


def get_date_range(month: int, year: int) -> tuple[str, str]:
    month_str = str(month).rjust(2, "0")

    start = f"{year}-{month_str}-01"

    _, end_day = calendar.monthrange(year, month)
    end_day_str = str(end_day).rjust(2, "0")
    end = f"{year}-{month_str}-{end_day_str}"

    return start, end


def get_season_date_range(season: str, year: int) -> tuple[str, str]:
    if season == "winter":
        start, _ = get_date_range(12, year - 1)
        _, end = get_date_range(2, year)
    elif season == "spring":
        start, _ = get_date_range(3, year)
        _, end = get_date_range(5, year)
    elif season == "summer":
        start, _ = get_date_range(6, year)
        _, end = get_date_range(8, year)
    elif season == "autumn":
        start, _ = get_date_range(9, year)
        _, end = get_date_range(11, year)
    else:
        err = f"Invalid season: {season}. Must be one of 'winter', 'spring', 'summer', or 'autumn'."
        raise ValueError(err)

    return start, end


def download_ee_raster(
    img: ee.image.Image,
    *,
    nodata: float,
    bounds: ee.geometry.Geometry,
    scale: int = 30,
) -> tuple[np.ndarray, str, Affine, float]:
    with tempfile.TemporaryDirectory() as tmpdir:
        fpath = Path(tmpdir) / "raster.tif"
        geemap.download_ee_image(
            img,
            fpath,
            scale=scale,
            region=bounds,
            crs="EPSG:4326",
            unmask_value=nodata,
        )
        with rio.open(fpath) as ds:
            data = ds.read(1)
            crs = ds.crs
            transform = ds.transform

            data = data.squeeze()
            data[data == nodata] = np.nan

            return data, str(crs), transform, np.nan
