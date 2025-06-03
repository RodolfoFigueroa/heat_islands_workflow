import dagster as dg


class PathResource(dg.ConfigurableResource):
    data_path: str
    population_grids_path: str
    centroid_path: str
