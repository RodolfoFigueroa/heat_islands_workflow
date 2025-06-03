import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
from affine import Affine
from rasterio.crs import CRS  # pylint: disable=no-name-in-module

import dagster as dg
from heat_islands_workflow.resources import PathResource


class BaseIOManager(dg.ConfigurableIOManager):
    path_resource: dg.ResourceDependency[PathResource]
    extension: str

    def _get_path(self, context: dg.InputContext | dg.OutputContext) -> Path:
        out_path = Path(self.path_resource.data_path) / "generated"
        fpath = out_path / "/".join(context.asset_key.path)

        if context.has_asset_partitions:
            if "|" in context.partition_key:
                a, b = context.partition_key.split("|")
                final_path = fpath / a / b

                if isinstance(context, dg.InputContext) and not final_path.exists():
                    final_path = fpath / a

            else:
                final_path = fpath / context.asset_partition_key
            final_path = final_path.with_suffix(final_path.suffix + self.extension)

        else:
            final_path = fpath.with_suffix(fpath.suffix + self.extension)

        return final_path


class RasterIOManager(BaseIOManager):
    def handle_output(
        self, context: dg.OutputContext, obj: tuple[np.ndarray, CRS | str, Affine],
    ):
        data, crs, transform = obj
        final_path = self._get_path(context)
        final_path.parent.mkdir(parents=True, exist_ok=True)

        with rio.open(
            final_path,
            "w",
            driver="GTiff",
            height=data.shape[0],
            width=data.shape[1],
            count=1,
            dtype=data.dtype,
            crs=crs,
            transform=transform,
            compress="lzw",
        ) as ds:
            if data.ndim == 2:
                ds.write(data, 1)
            else:
                for i in range(data.shape[0]):
                    ds.write(data[i], i + 1)

    def load_input(self, context: dg.InputContext) -> tuple[np.ndarray, CRS, Affine]:
        final_path = self._get_path(context)
        with rio.open(final_path) as src:
            return src.read(1), src.crs, src.transform


class PathIOManager(BaseIOManager):
    def handle_output(self, context: dg.OutputContext, obj):
        raise NotImplementedError("PathIOManager does not support output")

    def load_input(self, context: dg.InputContext) -> Path:
        return self._get_path(context)


class DataFrameIOManager(BaseIOManager):
    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame):
        fpath = self._get_path(context)
        fpath.parent.mkdir(parents=True, exist_ok=True)
        obj.to_csv(fpath)

    def load_input(self, context: dg.InputContext) -> gpd.GeoDataFrame:
        fpath = self._get_path(context)
        return gpd.read_file(fpath)


class GeoDataFrameIOManager(BaseIOManager):
    def handle_output(self, context: dg.OutputContext, obj: gpd.GeoDataFrame):
        fpath = self._get_path(context)
        fpath.parent.mkdir(parents=True, exist_ok=True)
        obj.to_file(fpath)

    def load_input(self, context: dg.InputContext) -> gpd.GeoDataFrame:
        fpath = self._get_path(context)
        return gpd.read_file(fpath)


class JSONIOManager(BaseIOManager):
    def handle_output(self, context: dg.OutputContext, obj: dict):
        fpath = self._get_path(context)
        fpath.parent.mkdir(parents=True, exist_ok=True)
        with open(fpath, "w", encoding="utf8") as f:
            json.dump(obj, f)

    def load_input(self, context: dg.InputContext) -> dict:
        fpath = self._get_path(context)
        with open(fpath, encoding="utf8") as f:
            return json.load(f)
