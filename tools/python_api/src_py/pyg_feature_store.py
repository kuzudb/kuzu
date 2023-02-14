from .connection import Connection
from typing import Dict, List, Optional, Tuple
import numpy as np

import torch
from torch import Tensor

from torch_geometric.data.feature_store import FeatureStore, TensorAttr
from torch_geometric.typing import FeatureTensorType


class KuzuFeatureStore(FeatureStore):
    def __init__(self, db):
        super().__init__()
        self.db = db
        self.connection = None
        self.store: Dict[Tuple[str, str], Tensor] = {}

    @staticmethod
    def key(attr: TensorAttr) -> str:
        return (attr.group_name, attr.attr_name)

    def _put_tensor(self, tensor: FeatureTensorType, attr: TensorAttr) -> bool:
        raise NotImplementedError

    def _get_tensor(self, attr: TensorAttr) -> Optional[FeatureTensorType]:
        table_name = attr.group_name
        attr_name = attr.attr_name
        indices = attr.index

        if not self.connection:
            self.connection = Connection(self.db)

        match_clause = "MATCH (item:%s)" % table_name
        return_clause = "RETURN item.%s" % attr_name

        if indices is None:
            where_clause = ""
        elif isinstance(indices, int):
            where_clause = "WHERE item.id = %d" % indices
        elif isinstance(indices, slice):
            if indices.step is None or indices.step == 1:
                where_clause = "WHERE item.id >= %d AND item.id < %d" % (
                    indices.start, indices.stop)
            else:
                where_clause = "WHERE item.id >= %d AND item.id < %d AND (item.id - %d) %% %d = 0" % (
                    indices.start, indices.stop, indices.start, indices.step)
        elif isinstance(indices, Tensor) or isinstance(indices, list) or isinstance(indices, np.ndarray):
            where_clause = "WHERE"
            for i in indices:
                where_clause += " item.id = %d OR" % int(i)
            where_clause = where_clause[:-3]
        else:
            raise ValueError("Invalid attr.index type: %s" % type(indices))

        query = "%s %s %s" % (match_clause, where_clause, return_clause)
        res = self.connection.execute(query)

        result_list = []
        while res.has_next():
            value_array = res.next()
            if len(value_array) == 1:
                value_array = value_array[0]
            result_list.append(value_array)

        return torch.tensor(result_list)

    def _remove_tensor(self, attr: TensorAttr) -> bool:
        raise NotImplementedError

    def _get_tensor_size(self, attr: TensorAttr) -> Tuple:
        return self._get_tensor(attr).size()
