from pathlib import Path

import ee

import dagster as dg
from heat_islands_workflow.defs.managers import (
    DataFrameIOManager,
    EarthEngineIOManager,
    GeoDataFrameIOManager,
    JSONIOManager,
    PathIOManager,
    RasterIOManager,
)
from heat_islands_workflow.defs.resources import PathResource, PostGISResource

ee.Initialize(project="ee-ursa-test")


@dg.definitions
def defs() -> dg.Definitions:
    main_defs = dg.load_from_defs_folder(project_root=Path(__file__).parent.parent)

    path_resource = PathResource(
        data_path=dg.EnvVar("DATA_PATH"),
        population_grids_path=dg.EnvVar("POPULATION_GRIDS_PATH"),
        centroid_path=dg.EnvVar("CENTROID_PATH"),
        ghsl_path=dg.EnvVar("GHSL_PATH"),
    )

    extra_defs = dg.Definitions(
        resources={
            "path_resource": path_resource,
            "postgis_resource": PostGISResource(
                host=dg.EnvVar("POSTGRES_HOST"),
                port=dg.EnvVar("POSTGRES_PORT"),
                user=dg.EnvVar("POSTGRES_USER"),
                password=dg.EnvVar("POSTGRES_PASSWORD"),
                db=dg.EnvVar("POSTGRES_DB"),
            ),
            "raster_io_manager": RasterIOManager(
                path_resource=path_resource,
                extension=".tif",
            ),
            "raster_path_io_manager": PathIOManager(
                path_resource=path_resource, extension=".tif"
            ),
            "csv_io_manager": DataFrameIOManager(
                path_resource=path_resource,
                extension=".csv",
            ),
            "gpkg_io_manager": GeoDataFrameIOManager(
                path_resource=path_resource,
                extension=".gpkg",
            ),
            "json_io_manager": JSONIOManager(
                path_resource=path_resource,
                extension=".json",
            ),
            "earthengine_io_manager": EarthEngineIOManager(
                path_resource=path_resource,
                extension=".json",
            ),
        },
    )
    return dg.Definitions.merge(main_defs, extra_defs)
