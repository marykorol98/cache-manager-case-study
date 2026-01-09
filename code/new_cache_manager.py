import os
from pathlib import Path
import pickle
from typing import Optional
import pandas as pd
import logging
import shutil
import polars as pl
from polars.dataframe.frame import DataFrame as pl_DataFrame
from polars.exceptions import ColumnNotFoundError
from pyarrow.lib import ArrowInvalid
import warnings

from graph.core.patameters.tools import hash_columns_list

logger = logging.getLogger("NodeCache")


class CacheManager:
    CACHE_ROOT = "CACHE/CACHE_OPT/projects"

    def __init__(
        self,
        storage_root: str = "",  # например: "/mnt/data/" или "s3://my-bucket/"
        cache_dir: str = "CACHE_test",
        nodes_dir: str = "nodes",
        *args,
        storage_options: Optional[
            dict
        ] = None,  # для s3: {"key": "...", "secret": "..."}
        **kwargs,
    ):
        self.metadata_store: list = []

        self.storage_root = storage_root.rstrip("/")
        self.cache_dir = cache_dir
        self.nodes_dir = nodes_dir
        self.storage_options = storage_options or {}
        self.path = ""
        self.node_id = None
        self.project_id: str | None = None
        self._remote_keys = []  # список ключей удаленных данных, чтоб не записывать их в кэш

    def create_folders(self, node_id: str, project_id: str | int):
        """Создание папок для хранения кэша данных узла"""
        if node_id and project_id:
            logger.info(
                "Creating path for project_id: %s, node_id: %s", project_id, node_id
            )
            self.path = f"{self.CACHE_ROOT}/{project_id}/{node_id}"
            create_folder(self.path)
            logger.info("Created path %s", self.path)

    def save_node(self, node_id: str, project_id: str, df: list):
        logger.info(
            "Start saving data for node %s in project_id %s", node_id, project_id
        )
        self.project_id = project_id

        self.node_id = node_id or self.node_id
        if self.node_id is None:
            raise ValueError("Fail to detect node for data saving")

        logger.info(
            "Start creating folders for node %s in project_id %s", node_id, project_id
        )

        self.create_folders(self.node_id, project_id=project_id)
        logger.info("Folders created for node %s in project_id %s", node_id, project_id)

        self.metadata_store = self.save_data_list(df, self.path, self._remote_keys)
        logger.info("Data saved in %s folder", self.path)

    def save_data_list(
        self, data_list: list, path: str, remote_keys: list = None, clean_names=False
    ) -> list[dict]:
        """"""
        remote_keys = remote_keys or []
        result_list = []
        for data_el in data_list:
            el = {"name": data_el["name"], "data": {}}
            for df_key, data in data_el["data"].items():
                if isinstance(data, str):  # ссылка на удаленные данные
                    remote_keys.append(df_key)
                    el["data"][df_key] = data
                elif not isinstance(data, pl_DataFrame):
                    df_key_clean = df_key.rsplit(":", 1)[-1] if clean_names else df_key

                    data_path = f"{path}/{df_key_clean}.parquet"
                    self.save_df_to_parquet(data, data_path)
                    el["data"][df_key_clean] = data_path
                else:
                    # Если пришёл polars.DataFrame, то это значит, что данные годятся только для просмотра - их сохранять не надо
                    warnings.warn(
                        "Предпринята попытка сохранить неполные данные кэша. "
                        "Пожалуйста, избегайте сохранения неполных экземпляров",
                        UserWarning,
                    )

            result_list.append(el)

        return result_list

    def save_df_to_parquet(self, df: pd.DataFrame | bytes, data_path: str):
        if isinstance(df, bytes):
            df = pickle.loads(df)
        if isinstance(df, pd.DataFrame):
            df.to_parquet(data_path, index=False, storage_options=self.storage_options)

    def read_data_cache(self, **kwargs) -> list[pd.DataFrame]:
        """Читает узел, с кэшированием при необходимости"""
        logger.info(
            "Loading data for project_id: %s, node_id: %s",
            self.project_id,
            self.node_id,
        )

        data = self.load_data_list(
            data_list=self.metadata_store, remote_keys=self._remote_keys, **kwargs
        )

        logger.info(
            "Data loaded for project_id: %s, node_id: %s",
            self.project_id,
            self.node_id,
        )

        return data

    def load_data_list(
        self,
        data_list: list,
        remote_keys: list = None,
        columns: Optional[list[str]] = None,
        cols_mapping=None,
        with_values: bool = True,
        lazy_read: bool = False,
        **kwargs,
    ) -> list[pd.DataFrame]:
        cols_mapping, result_list = cols_mapping or {}, []
        for el in data_list:
            data_el = {"name": el["name"], "data": {}}
            for df_key, path in el["data"].items():
                if (
                    remote_keys is not None and df_key in remote_keys
                ):  # удаленные данные, ничего из кэша не достаем, просто берем path
                    data_el["data"][df_key] = path
                else:
                    if lazy_read:
                        data = self.load_df_parquet_lazy(
                            path=path, columns=columns, **kwargs
                        )
                    else:
                        try:
                            data = (
                                self.load_df_parquet(path)
                                if with_values
                                else pd.DataFrame()
                            )  # нам не всегда нужны значения
                        except Exception as err:
                            raise RuntimeError(
                                "Вы поймали самую редкую ошибку в SMILE, Поздравляю вас. Этот граф скорее всего сломан, либо перезагрузите граф, либо создайте новый. Мы уже работаем над этим, простите за неудобства!"
                            ) from err

                    if (key := df_key.replace("_", ":")) in cols_mapping:  # proba
                        df_key = cols_mapping.pop(key)
                    elif isinstance(data, pd.DataFrame):
                        data = data.rename(columns=cols_mapping)

                    data_el["data"][df_key.replace("_", ":")] = data

            result_list.append(data_el)

        self.lazy_read = False

        return result_list

    def load_df_parquet(
        self,
        data_path: str,
    ) -> pd.DataFrame:
        return pd.read_parquet(data_path, storage_options=self.storage_options)

    def load_df_parquet_lazy(
        self,
        path: str,
        columns: list | None = None,
        row_start: int = 0,
        row_length: int | None = None,
        prefix: str = "",
        **kwargs,
    ) -> "pl_DataFrame":
        """
        Читает Parquet-файл в Polars DataFrame с возможностью подгрузки
        части строк и выбора столбцов.

        В отличие от pandas, Polars обеспечивает более быструю и эффективную загрузку
        данных за счёт колоночного формата (Arrow) и многопоточности.
        Можно загрузить весь датасет или только указанные столбцы и строки.

        Parameters
        ----------
        path : str
            Путь к Parquet-файлу.
        columns : list | None, optional
            Список столбцов для загрузки. Если None (по умолчанию),
            загружаются все столбцы.
        row_start : int, default 0
            Смещение от начала файла — с какой строки начинать чтение.
        row_length : int | None, optional
            Количество строк для чтения. Если None (по умолчанию), читаются все строки
            начиная с `row_start`.
        prefix: str | None, optional
           префикс для конвертации названий колонок
        **kwargs : dict
            Дополнительные параметры, передаваемые в Polars (например, для кастомизации чтения).

        Returns
        -------
        pl.DataFrame
            Объект Polars DataFrame с загруженными данными.

        Notes
        -----
        - Ограничение по столбцам (`columns`) позволяет ускорить чтение и снизить
        потребление памяти.
        - Ограничение по строкам (`row_start` и `row_length`) полезно для постраничной
        выборки или при работе с большими файлами.
        - Для очень больших датасетов можно использовать `pl.scan_parquet(path)`,
        чтобы работать в ленивом режиме без загрузки всего файла в память.

        Examples
        --------
        >>> # Прочитать первые 100 строк всех столбцов
        >>> df = self.load_df_parquet_lazy("data.parquet", row_length=100)

        >>> # Прочитать 100 строк начиная с 1000-й
        >>> df = self.load_df_parquet_lazy("data.parquet", row_start=1000, row_length=100)

        >>> # Прочитать только столбцы "id" и "value"
        >>> df = self.load_df_parquet_lazy("data.parquet", columns=["id", "value"])
        """

        if columns is None:
            return pl.read_parquet(path, n_rows=row_length, row_index_offset=row_start)

        # Пробуем доставать как по "чистым" фичам, так и по "грязным"
        for cols in [columns, hash_columns_list(columns, prefix)]:
            try:
                return pl.read_parquet(
                    path, columns=cols, n_rows=row_length, row_index_offset=row_start
                )
            except (ColumnNotFoundError, ArrowInvalid):
                continue

        # если колонки не найдены ни одним из способов, то читаем все
        return pl.read_parquet(path, n_rows=row_length, row_index_offset=row_start)

    @property
    def is_empty(self):
        return not os.path.isdir(self.path)

    @staticmethod
    def delete_project_cache(project_id: str | int) -> None:
        """
        Delete all cached files for a given project ID from the filesystem.

        :param project_id: Identifier of the project whose cache (nodes) should be deleted.
        """
        logger.info("Deleting project cache for project_id: %s", project_id)
        node_path = Path(f"{CacheManager.CACHE_ROOT}/{project_id}")

        if node_path.exists() and node_path.is_dir():
            shutil.rmtree(node_path)
            logger.info("Cache deleted: %s", node_path)
        else:
            logger.info("Path not found. Failed to delete: %s", node_path)
            return

    def drop(self):
        """
        Стирает всю мету
        """
        self.metadata_store = []
        self._remote_keys.clear()
