import dagster as dg
from heat_islands_workflow.assets.raster import monthly, suhi, world_cover

defs = dg.Definitions(
    assets=(dg.load_assets_from_modules([monthly, suhi, world_cover])),
)
