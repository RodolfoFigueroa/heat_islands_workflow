import dagster as dg
from heat_islands_workflow.assets import bounds, polygons, raster, stats
from heat_islands_workflow.managers import (
    DataFrameIOManager,
    GeoDataFrameIOManager,
    JSONIOManager,
    PathIOManager,
    RasterIOManager,
)
from heat_islands_workflow.resources import PathResource

# Resources
path_resource = PathResource(
    data_path=dg.EnvVar("DATA_PATH"),
    population_grids_path=dg.EnvVar("POPULATION_GRIDS_PATH"),
    centroid_path=dg.EnvVar("CENTROID_PATH"),
)


# IO Managers
raster_path_io_manager = PathIOManager(path_resource=path_resource, extension=".tif")

raster_io_manager = RasterIOManager(
    path_resource=path_resource,
    extension=".tif",
)

csv_io_manager = DataFrameIOManager(
    path_resource=path_resource,
    extension=".csv",
)

gpkg_io_manager = GeoDataFrameIOManager(
    path_resource=path_resource,
    extension=".gpkg",
)

json_io_manager = JSONIOManager(
    path_resource=path_resource,
    extension=".json",
)


# Definitions
defs = dg.Definitions.merge(
    dg.Definitions(
        assets=(
            dg.load_assets_from_modules([stats], group_name="stats")
            + dg.load_assets_from_modules([bounds], group_name="bounds")
            + dg.load_assets_from_modules([polygons], group_name="polygons")
        ),
        resources=dict(
            path_resource=path_resource,
            raster_io_manager=raster_io_manager,
            raster_path_io_manager=raster_path_io_manager,
            gpkg_io_manager=gpkg_io_manager,
            csv_io_manager=csv_io_manager,
            json_io_manager=json_io_manager,
        ),
    ),
    raster.defs,
)
