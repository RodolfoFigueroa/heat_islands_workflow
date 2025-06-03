import numpy as np
import requests
from affine import Affine


def fetch_raster(url: str, params: dict) -> tuple[np.ndarray, str, Affine]:
    response = requests.get(url, params=params, timeout=500)

    if response.status_code != 200:
        raise Exception(
            f"Error fetching raster data: {response.status_code} - {response.text}",
        )

    response_json = response.json()

    data = np.reshape(
        response_json["data"], (response_json["height"], response_json["width"]),
    )
    crs = response_json["crs"]
    transform = Affine(*response_json["transform"])

    return data, crs, transform
