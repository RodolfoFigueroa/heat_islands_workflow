import geopandas as gpd
import shapely


def buffer_bounds(
    df: gpd.GeoDataFrame, buffer_size: float = 10_000,
) -> tuple[float, float, float, float]:
    xmin, ymin, xmax, ymax = df.total_bounds
    xmin -= buffer_size
    ymin -= buffer_size
    xmax += buffer_size
    ymax += buffer_size

    xmin, ymin, xmax, ymax = (
        gpd.GeoSeries([shapely.box(xmin, ymin, xmax, ymax)], crs=df.crs)
        .to_crs("EPSG:4326")
        .total_bounds
    )
    return xmin, ymin, xmax, ymax
