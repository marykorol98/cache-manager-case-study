import os
from pathlib import Path
import pickle
import shutil
import warnings
from abc import ABC
from typing import Dict, List

import h5py
import pandas as pd

from smile_ml_core.data.services.reader import convert_wkt_if_geo, convert_geo_data
from smile_ml_core.data.services.tools import create_folder

from graph.config import settings
import logging


logger = logging.getLogger("NodeCache")


class StorageFileHandler(ABC):
    def save(self, data, key: str, path: str):
        raise NotImplementedError()

    def load(self, key: str, path: str):
        raise NotImplementedError()


class PickleStorageFileHandler(StorageFileHandler):
    def save(self, data, key: str, path: str):
        if isinstance(data, pd.DataFrame):
            data.to_pickle(path)
        else:
            with open(path, "wb") as f:
                pickle.dump(data, f)

    def load(self, key: str, path: str):
        with open(path, "rb") as f:
            return pickle.load(f)


class H5pyStorageFileHandler(StorageFileHandler):
    def __init__(self):
        super().__init__()

    def save(self, data, key: str, path: str):
        if isinstance(data, bytes):
            data = pickle.loads(data)
        if isinstance(data, pd.DataFrame):
            # FIXME: Columns index has to be unique for fixed format
            try:
                data.to_hdf(path, key=key, format="table")
            except TypeError:
                object_columns = data.select_dtypes("object").columns.to_list()

                # Сохраним mixed columns в виде строковых и кинем ворнинг: в дальнейшем пользователи смогут вручную
                # поменять на нужный тип либо изменить собственный пайплайн вычислений
                data[object_columns] = data[object_columns].astype("str")
                if object_columns := list(
                    map(lambda c: c.rsplit(":", 1)[-1], object_columns)
                ):
                    warnings.warn(
                        f"There was an error saving intermediate data due to mixed data types. "
                        f'Failed to localize columns, but the problem might be in columns: {", ".join(object_columns)}. '
                        f"Check that there are no mixed data types or contact your administrator."
                    )
                data.to_hdf(path, key=key, format="table")

            except ValueError:
                s = data.columns.to_series()
                data.columns = s + s.groupby(s).cumcount().astype(str).replace(
                    {"0": ""}
                )
                data.to_hdf(path, key=key, format="table")
            except AttributeError:
                # Геоданные для сохранения преобразовываем
                convert_geo_data(data).to_hdf(path, key=key, format="table")
        else:
            with h5py.File(path, "w") as h5f:
                h5f.create_dataset(key, data=data)

    def load(self, key: str, path: str):
        try:  # pd.DataFrame
            with pd.HDFStore(path, "r") as store:
                data = store[key]
        except:  # numpy.ndarray  # noqa: E722
            with h5py.File(path, "r") as h5f:
                data = h5f[key][:]

        data = convert_wkt_if_geo(data)  # преобразовываем обратно, если есть геоданные
        return data


def get_storage_instance():
    if settings.DATA_CACHE_STORAGE_TOOL == "pickle":
        return PickleStorageFileHandler()
    else:  # by default is h5py
        return H5pyStorageFileHandler()


class Cache:
    CACHE_ROOT = "CACHE/data/nodes"
    # handler for saving and loading data from storage
    storage_handler = get_storage_instance()

    def __init__(self):
        self.data = {"input": {}, "output": []}
        self.path = ""
        self.node_id = None
        self.project_id: str | None = None
        self._remote_keys = []  # список ключей удаленных данных, чтоб не записывать их в кэш

    def __setstate__(self, state):
        self.__dict__.update(state)

        if not hasattr(self, "storage_handler"):  # for backward compatibility
            setattr(self, "storage_handler", get_storage_instance())

    @property
    def is_empty(self):
        return not os.path.isdir(self.path)

    def save_data(self, data: dict, node_id: str | None, project_id: str):
        logger.info(
            "Start saving data for node %s in project_id %s", node_id, project_id
        )
        self.project_id = project_id

        if node_id is not None:
            self.node_id = node_id
        elif self.node_id is None:
            raise Exception("Fail to detect node for data saving")

        logger.info(
            "Start creating folders for node %s in project_id %s", node_id, project_id
        )
        self.create_folders(self.node_id, project_id=project_id)
        logger.info("Folders created for node %s in project_id %s", node_id, project_id)

        logger.info("Saving data in %s/output folder", self.path)
        self.data["output"] = self.save_data_list_to_hd(
            data_list=data["output"],
            path=f"{self.path}/output",
            remote_keys=self._remote_keys,
        )
        logger.info("Data saved in %s/output folder", self.path)

        logger.info("Start saving each element in input to %s/input", self.path)
        for id_, data_list in data["input"].items():
            id_ = id_.replace(":", "_")
            el_path = f"{self.path}/input/{id_}"

            logger.info("Creating %s element folder if not exist", el_path)
            os.makedirs(el_path, exist_ok=True)
            logger.info("%s folder created", el_path)

            logger.info("Saving all keys to folder %s", el_path)
            self.data["input"][id_] = self.save_data_list_to_hd(
                data_list=data_list, remote_keys=self._remote_keys, path=el_path
            )
            logger.info(
                "All keys for node: %s,  in project: %s, with id_: %s saved to folder %s",
                node_id,
                project_id,
                id_,
                el_path,
            )

        logger.info("All data saved for node %s in project_id %s", node_id, project_id)

    def load_data(self) -> dict:
        logger.info(
            "Loading data for project_id: %s, node_id: %s",
            self.project_id,
            self.node_id,
        )
        data = {
            "input": {},
            "output": self.load_data_list_from_hd(
                data_list=self.data["output"], remote_keys=self._remote_keys
            ),
        }
        logger.info(
            "Output data loaded for project_id: %s, node_id: %s. Input data empty on this step.",
            self.project_id,
            self.node_id,
        )
        logger.info("Starting load input data")
        for id_, data_list in self.data["input"].items():
            logger.info("Load input data for id_: %s", id_)
            data["input"][id_.replace("_", ":")] = self.load_data_list_from_hd(
                data_list, self._remote_keys
            )
            logger.info("Data loaded for id_: %s", id_)

        logger.info("All data loaded")

        return data

    @classmethod
    def save_data_list_to_hd(
        cls, data_list: list, path: str, remote_keys: list = None, clean_names=False
    ) -> List[Dict]:
        """"""
        result_list = []
        for data_el in data_list:
            el = {"name": data_el["name"], "data": {}}
            for df_key, data in data_el["data"].items():
                if isinstance(data, str):  # ссылка на удаленные данные
                    if remote_keys is not None:
                        remote_keys.append(df_key)
                        el["data"][df_key] = data
                else:
                    df_key = (
                        df_key.rsplit(":", 1)[-1]
                        if clean_names
                        else df_key.replace(":", "_")
                    )
                    if not df_key.endswith(el["name"]):
                        df_key = df_key + ":" + str(el["name"])
                    data_path = f"{path}/{df_key}.h5"
                    cls.storage_handler.save(data, df_key, data_path)
                    el["data"][df_key] = data_path

            result_list.append(el)

        return result_list

    @classmethod
    def load_data_list_from_hd(
        cls, data_list: list, remote_keys: list = None, cols_mapping=None
    ):
        cols_mapping, result_list = cols_mapping or {}, []
        for el in data_list:
            data_el = {"name": el["name"], "data": {}}
            for df_key, path in el["data"].items():
                if (
                    remote_keys is not None and df_key in remote_keys
                ):  # удаленные данные, ничего из кэша не достаем, просто берем path
                    data_el["data"][df_key] = path
                else:
                    try:
                        data = cls.storage_handler.load(df_key, path)
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

        return result_list

    def create_folders(self, node_id: str, project_id: str | int):
        """Создание папок для хранения кэша данных узла"""
        if node_id and project_id:
            logger.info(
                "Creating path for project_id: %s, node_id: %s", project_id, node_id
            )
            self.path = f"{self.CACHE_ROOT}/{project_id}/{node_id}"
            create_folder(f"{self.path}/input")
            create_folder(f"{self.path}/output")
            logger.info("Created path %s", self.path)

    @staticmethod
    def delete_project_cache(project_id: str | int) -> None:
        """
        Delete all cached files for a given project ID from the filesystem.

        :param project_id: Identifier of the project whose cache (nodes) should be deleted.
        """
        logger.info("Deleting project cache for project_id: %s", project_id)
        node_path = Path(f"{Cache.CACHE_ROOT}/{project_id}")

        if node_path.exists() and node_path.is_dir():
            shutil.rmtree(node_path)
            logger.info("Cache deleted: %s", node_path)
        else:
            logger.info("Path not found. Failed to delete: %s", node_path)
            return

    def drop(self):
        self.data["input"] = {}
        self.data["output"] = []

        self._remote_keys.clear()
