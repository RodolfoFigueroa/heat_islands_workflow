import json
from collections.abc import Sequence
from pathlib import Path

import ee
import ee.deserializer
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
from affine import Affine
from rasterio.crs import CRS

import dagster as dg
from heat_islands_workflow.defs.resources import PathResource


def process_partition_key(partition_key: str, root_path: Path, extension: str) -> Path:
    partition_key_split = partition_key.split("|")
    final_path = root_path / "/".join(partition_key_split)
    return final_path.with_suffix(final_path.suffix + extension)


def process_multiple_partitions(
    partition_keys: Sequence[str],
    root_path: Path,
    extension: str,
) -> dict[str, Path]:
    path_map = {}
    for key in partition_keys:
        path_map[key] = process_partition_key(key, root_path, extension)
    return path_map


class BaseIOManager(dg.ConfigurableIOManager):
    path_resource: dg.ResourceDependency[PathResource]
    extension: str

    def _get_path(self, context: dg.InputContext | dg.OutputContext) -> Path:
        out_path = Path(self.path_resource.data_path) / "generated"
        fpath = out_path / "/".join(context.asset_key.path)

        if context.has_asset_partitions:
            if len(context.asset_partition_keys) == 1:
                final_path = process_partition_key(
                    context.asset_partition_keys[0],
                    fpath,
                    self.extension,
                )
            # Multiple partitions
            else:
                err = "Multiple partitions not supported in this context."
                raise NotImplementedError(err)

        else:
            final_path = fpath.with_suffix(fpath.suffix + self.extension)

        return final_path

    def _get_path_multiple(
        self,
        context: dg.InputContext | dg.OutputContext,
    ) -> Path | dict[str, Path]:
        out_path = Path(self.path_resource.data_path) / "generated"
        fpath = out_path / "/".join(context.asset_key.path)

        if context.has_asset_partitions:
            if len(context.asset_partition_keys) == 1:
                final_path = process_partition_key(
                    context.asset_partition_keys[0],
                    fpath,
                    self.extension,
                )
                print(final_path)
            # Multiple partitions
            else:
                final_path = process_multiple_partitions(
                    context.asset_partition_keys,
                    fpath,
                    self.extension,
                )
                print("A" * 80)
                print(final_path)

        else:
            final_path = fpath.with_suffix(fpath.suffix + self.extension)

        return final_path


class RasterIOManager(BaseIOManager):
    def handle_output(
        self,
        context: dg.OutputContext,
        obj: tuple[np.ndarray, str, Affine, float],
    ) -> None:
        data, crs, transform, nodata = obj
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
            nodata=nodata,
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
            return src.read(1), str(src.crs), src.transform


class PathIOManager(BaseIOManager):
    def handle_output(self, context: dg.OutputContext, obj) -> None:
        err = "PathIOManager does not support output"
        raise NotImplementedError(err)

    def load_input(self, context: dg.InputContext) -> Path:
        return self._get_path(context)


class DataFrameIOManager(BaseIOManager):
    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame) -> None:
        fpath = self._get_path(context)
        fpath.parent.mkdir(parents=True, exist_ok=True)
        obj.to_csv(fpath)

    def load_input(self, context: dg.InputContext) -> gpd.GeoDataFrame:
        fpath = self._get_path(context)
        return gpd.read_file(fpath)


class GeoDataFrameIOManager(BaseIOManager):
    def handle_output(self, context: dg.OutputContext, obj: gpd.GeoDataFrame) -> None:
        fpath = self._get_path(context)
        fpath.parent.mkdir(parents=True, exist_ok=True)
        obj.to_file(fpath)

    def load_input(self, context: dg.InputContext) -> gpd.GeoDataFrame:
        fpath = self._get_path(context)
        return gpd.read_file(fpath)


class JSONIOManager(BaseIOManager):
    def handle_output(self, context: dg.OutputContext, obj: dict) -> None:
        fpath = self._get_path(context)
        fpath.parent.mkdir(parents=True, exist_ok=True)
        with open(fpath, "w", encoding="utf8") as f:
            json.dump(obj, f)

    def load_input(self, context: dg.InputContext) -> dict | dict[str, dict]:
        fpath = self._get_path_multiple(context)

        if isinstance(fpath, Path):
            with open(fpath, encoding="utf8") as f:
                return json.load(f)

        if isinstance(fpath, dict):
            out = {}
            for key, value in fpath.items():
                with open(value, encoding="utf8") as f:
                    out[key] = json.load(f)
            return out

        err = f"Unsupported type: {type(fpath)}"
        raise TypeError(err)


class EarthEngineIOManager(BaseIOManager):
    def handle_output(
        self,
        context: dg.OutputContext,
        obj: ee.image.Image | ee.geometry.Geometry,
    ) -> None:
        fpath = self._get_path(context)
        fpath.parent.mkdir(exist_ok=True, parents=True)

        serialized = json.loads(obj.serialize())

        with fpath.open("w") as f:
            json.dump(serialized, f)

    def _decode_json(self, fpath) -> ee.image.Image | ee.geometry.Geometry:
        with fpath.open() as f:
            serialized = json.load(f)

        deserialized = ee.deserializer.decode(serialized)

        if isinstance(deserialized, ee.image.Image | ee.geometry.Geometry):
            return deserialized

        err = f"Unsupported type: {type(deserialized)}"
        raise TypeError(err)

    def load_input(
        self,
        context: dg.InputContext,
    ) -> (
        ee.image.Image
        | ee.geometry.Geometry
        | dict[str, ee.image.Image]
        | dict[str, ee.geometry.Geometry]
    ):
        fpath = self._get_path_multiple(context)

        if isinstance(fpath, Path):
            return self._decode_json(fpath)

        if isinstance(fpath, dict):
            out = {}
            for key, value in fpath.items():
                out[key] = self._decode_json(value)
            return out

        err = f"Unsupported type: {type(fpath)}"
        raise TypeError(err)
