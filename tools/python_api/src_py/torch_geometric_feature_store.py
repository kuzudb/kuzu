from .connection import Connection
from typing import List, Optional, Tuple
from .types import Type
import numpy as np

import torch
import multiprocessing
from torch import Tensor

from torch_geometric.data.feature_store import FeatureStore, TensorAttr
from torch_geometric.typing import FeatureTensorType


class KuzuFeatureStore(FeatureStore):
    def __init__(self, db, num_threads):
        super().__init__()
        self.db = db
        self.connection = None
        self.node_properties_cache = {}
        if num_threads is None:
            num_threads = multiprocessing.cpu_count()
        self.num_threads = num_threads

    def __get_connection(self):
        if not self.connection:
            self.connection = Connection(self.db, self.num_threads)
        return self.connection

    def _put_tensor(self, tensor: FeatureTensorType, attr: TensorAttr) -> bool:
        raise NotImplementedError

    def _get_tensor(self, attr: TensorAttr) -> Optional[FeatureTensorType]:
        table_name = attr.group_name
        attr_name = attr.attr_name
        attr_info = self.__get_node_property(table_name, attr_name)
        if (not attr_info["type"] in [
            Type.INT64.value, Type.INT32.value, Type.INT16.value,
            Type.DOUBLE.value, Type.FLOAT.value, Type.BOOL.value
        ]) or (attr_info["dimension"] > 0 and "shape" not in attr_info):
            return self.__get_tensor_by_query(attr)
        return self.__get_tensor_by_scan(attr)

    def __get_tensor_by_scan(self, attr: TensorAttr) -> Optional[FeatureTensorType]:
        table_name = attr.group_name
        attr_name = attr.attr_name
        indices = attr.index

        if indices is None:
            shape = self.get_tensor_size(attr)
            if indices is None:
                indices = np.arange(shape[0], dtype=np.uint64)
        elif isinstance(indices, slice):
            if indices.step is None or indices.step == 1:
                indices = np.arange(
                    indices.start, indices.stop, dtype=np.uint64)
            else:
                indices = np.arange(indices.start, indices.stop,
                                    indices.step, dtype=np.uint64)
        elif isinstance(indices, int):
            indices = np.array([indices])

        if table_name not in self.node_properties_cache:
            self.node_properties_cache[table_name] = self.connection._get_node_property_names(
                table_name)
        attr_info = self.node_properties_cache[table_name][attr_name]

        flat_dim = 1
        if attr_info["dimension"] > 0:
            for i in range(attr_info["dimension"]):
                flat_dim *= attr_info["shape"][i]
        scan_result = self.connection.database._scan_node_table(
            table_name, attr_name, attr_info["type"], flat_dim, indices, self.num_threads)

        if attr_info['dimension'] > 0 and "shape" in attr_info:
            result_shape = (len(indices),) + attr_info["shape"]
            scan_result = scan_result.reshape(result_shape)

        result = torch.from_numpy(scan_result)
        return result

    def __get_tensor_by_query(self, attr: TensorAttr) -> Optional[FeatureTensorType]:
        table_name = attr.group_name
        attr_name = attr.attr_name
        indices = attr.index

        self.__get_connection()

        match_clause = "MATCH (item:%s)" % table_name
        return_clause = "RETURN item.%s" % attr_name

        if indices is None:
            where_clause = ""
        elif isinstance(indices, int):
            where_clause = "WHERE offset(id(item)) = %d" % indices
        elif isinstance(indices, slice):
            if indices.step is None or indices.step == 1:
                where_clause = "WHERE offset(id(item)) >= %d AND offset(id(item)) < %d" % (
                    indices.start, indices.stop)
            else:
                where_clause = "WHERE offset(id(item)) >= %d AND offset(id(item)) < %d AND (offset(id(item)) - %d) %% %d = 0" % (
                    indices.start, indices.stop, indices.start, indices.step)
        elif isinstance(indices, Tensor) or isinstance(indices, list) or isinstance(indices, np.ndarray) or isinstance(indices, tuple):
            where_clause = "WHERE"
            for i in indices:
                where_clause += " offset(id(item)) = %d OR" % int(i)
            where_clause = where_clause[:-3]
        else:
            raise ValueError("Invalid attr.index type: %s" % type(indices))

        query = "%s %s %s" % (match_clause, where_clause, return_clause)
        res = self.connection.execute(query)

        result_list = []
        while res.has_next():
            value_array = res.get_next()
            if len(value_array) == 1:
                value_array = value_array[0]
            result_list.append(value_array)
        try:
            tensor_result = torch.tensor(result_list)
            return tensor_result
        except:
            return result_list

    def _remove_tensor(self, attr: TensorAttr) -> bool:
        raise NotImplementedError

    def _get_tensor_size(self, attr: TensorAttr) -> Tuple:
        self.__get_connection()
        length_query = "MATCH (item:%s) RETURN count(item)" % attr.group_name
        res = self.connection.execute(length_query)
        length = res.get_next()[0]
        attr_info = self.__get_node_property(attr.group_name, attr.attr_name)
        if attr_info["dimension"] == 0:
            return (length,)
        else:
            return (length,) + attr_info["shape"]

    def __get_node_property(self, table_name: str, attr_name: str) -> str:
        if table_name in self.node_properties_cache and attr_name in self.node_properties_cache[table_name]:
            return self.node_properties_cache[table_name][attr_name]
        self.__get_connection()
        if table_name not in self.node_properties_cache:
            self.node_properties_cache[table_name] = self.connection._get_node_property_names(
                table_name)
        if attr_name not in self.node_properties_cache[table_name]:
            raise ValueError("Attribute %s not found in group %s" %
                             (attr_name, table_name))
        attr_info = self.node_properties_cache[table_name][attr_name]
        return attr_info

    def get_all_tensor_attrs(self) -> List[TensorAttr]:
        result_list = []
        self.__get_connection()
        for table_name in self.connection._get_node_table_names():
            if table_name not in self.node_properties_cache:
                self.node_properties_cache[table_name] = self.connection._get_node_property_names(
                    table_name)
            for attr_name in self.node_properties_cache[table_name]:
                if self.node_properties_cache[table_name][attr_name]["type"] in [
                    Type.INT64.value, Type.INT32.value, Type.INT16.value,
                    Type.DOUBLE.value, Type.FLOAT.value, Type.BOOL.value
                ] and (
                    self.node_properties_cache[table_name][attr_name]["dimension"] == 0
                    or "shape" in self.node_properties_cache[table_name][attr_name]
                ):
                    result_list.append(TensorAttr(table_name, attr_name))
        return result_list
