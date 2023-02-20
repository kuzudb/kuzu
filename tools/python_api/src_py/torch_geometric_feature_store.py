from .connection import Connection
from typing import Dict, List, Optional, Tuple
import numpy as np

import torch
from torch import Tensor

from torch_geometric.data.feature_store import FeatureStore, TensorAttr
from torch_geometric.typing import FeatureTensorType

import time


class KuzuFeatureStore(FeatureStore):
    def __init__(self, db):
        super().__init__()
        self.db = db
        self.connection = None

    def _put_tensor(self, tensor: FeatureTensorType, attr: TensorAttr) -> bool:
        raise NotImplementedError

    def _get_tensor(self, attr: TensorAttr) -> Optional[FeatureTensorType]:
        table_name = attr.group_name
        attr_name = attr.attr_name·
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
            where_clause = "WHERE"
            for i in indices:
                where_clause += " offset(id(item)) = %d OR" % int(i)
            where_clause = where_clause[:-3]
        else:
            raise ValueError("Invalid attr.index type: %s" % type(indices))

        query = "%s %s %s" % (match_clause, where_clause, return_clause)
        start = time.time()
        res = self.connection.execute(query)
        end = time.time()
        print("Query time: %f" % (end - start))

        result_list = []
        start = time.time()
        while res.has_next():
            value_array = res.get_next()
            if len(value_array) == 1:
                value_array = value_array[0]
            result_list.append(value_array)
        end = time.time()
        print("Fetch time: %f" % (end - start))

        try:
            start = time.time()
            tensor_result =  torch.tensor(result_list)
            end = time.time()
            print("Convert time: %f" % (end - start))
            return tensor_result
        except:
            return result_list

    def _remove_tensor(self, attr: TensorAttr) -> bool:
        raise NotImplementedError

    def _get_tensor_size(self, attr: TensorAttr) -> Tuple:
        return self._get_tensor(attr).size()
