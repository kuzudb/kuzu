from .connection import Connection
from typing import Dict, List, Optional, Tuple
from .types import Type
import numpy as np

import torch
from torch import Tensor

from torch_geometric.data.feature_store import FeatureStore, TensorAttr
from torch_geometric.typing import FeatureTensorType

import time
import os


class KuzuFeatureStore(FeatureStore):
    def __init__(self, db):
        super().__init__()
        self.db = db
        self.connection = None
        self.node_properties_cache = {}

    def __getstate__(self):
        state = {
            "connection": None,
            "node_properties_cache": self.node_properties_cache,
            "db": self.db.__getstate__()
        }
        return state

    def _put_tensor(self, tensor: FeatureTensorType, attr: TensorAttr) -> bool:
        raise NotImplementedError

    def _get_tensor(self, attr: TensorAttr) -> Optional[FeatureTensorType]:
        table_name = attr.group_name
        attr_name = attr.attr_name
        indices = attr.index

        if not self.connection:
            self.connection = Connection(self.db, num_threads=1)

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
            if table_name not in self.node_properties_cache:
                self.node_properties_cache[table_name] = self.connection._get_node_property_names(
                    table_name)
            attr_info = self.node_properties_cache[table_name][attr_name]
            if (not attr_info["type"] in [
                Type.INT64.value, Type.DOUBLE.value, Type.FLOAT.value, Type.BOOL.value
            ]) or (attr_info["dimension"] > 0 and "shape" not in attr_info):
                where_clause = "WHERE"
                for i in indices:
                    where_clause += " offset(id(item)) = %d OR" % int(i)
                where_clause = where_clause[:-3]
            else:
                scan_result = self.connection.database._scan_node_table(
                    table_name, attr_name, attr_info["type"], indices)
                start = time.time()
                if attr_info['dimension'] > 0 and "shape" in attr_info:
                    result_shape = (len(indices),) + attr_info["shape"]
                    scan_result = scan_result.reshape(result_shape)
                end = time.time() - start
                # print("Reshape time: ", end)

                start = time.time()
                result = torch.from_numpy(scan_result)
                end = time.time() - start
                # print("Tensor cast time: ", end)
                return result
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
        return self._get_tensor(attr).size()

    def get_all_tensor_attrs(self) -> List[TensorAttr]:
        retult_list = []
        if not self.connection:
            self.connection = Connection(self.db, num_threads=1)
        for table_name in self.connection._get_node_table_names():
            if table_name not in self.node_properties_cache:
                self.node_properties_cache[table_name] = self.connection._get_node_property_names(
                    table_name)
            for attr_name in self.node_properties_cache[table_name]:
                retult_list.append(TensorAttr(table_name, attr_name))
        return retult_list

    def __close_connection(self):
        if self.connection:
            del self.connection
            self.connection = None
        if self.db:
            del self.db._database
