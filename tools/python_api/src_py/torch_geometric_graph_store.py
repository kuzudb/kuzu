from typing import Dict, List, Optional, Tuple

import torch
import multiprocessing
from torch_geometric.data.graph_store import EdgeAttr, GraphStore, EdgeLayout
from torch_geometric.typing import EdgeTensorType
from .connection import Connection
import numpy as np

REL_BATCH_SIZE = 1000000


class Rel:
    def __init__(self, edge_type, layout, is_sorted, size, materialized=False, edge_index=None):
        self.edge_type = edge_type
        self.layout = layout
        self.is_sorted = is_sorted
        self.size = size
        self.materialized = materialized
        self.edge_index = edge_index


class KuzuGraphStore(GraphStore):
    def __init__(self, db, num_threads):
        super().__init__()
        self.db = db
        self.connection = None
        self.store = {}
        if num_threads is None:
            num_threads = multiprocessing.cpu_count()
        self.num_threads = num_threads
        self.__populate_edge_attrs()

    def __getstate__(self):
        state = {
            "connection": None,
            "store": self.store,
            "db": self.db.__getstate__()
        }
        return state

    @staticmethod
    def key(attr: EdgeAttr) -> Tuple:
        return (attr.edge_type, attr.layout.value, attr.is_sorted)

    def _put_edge_index(self, edge_index: EdgeTensorType,
                        edge_attr: EdgeAttr) -> bool:
        key = self.key(edge_attr)
        if key in self.store:
            self.store[key].edge_index = edge_index
            self.store[key].materialized = True
            self.store[key].size = edge_attr.size
        else:
            self.store[key] = Rel(key[0], key[1], key[2], edge_attr.size, True,
                                  edge_index)

    def _get_edge_index(self, edge_attr: EdgeAttr) -> Optional[EdgeTensorType]:
        key = self.key(edge_attr)
        if key[1] == None:
            key[1] = EdgeLayout.COO.value
        if key[2] == None:
            key[2] = False
        if key in self.store:
            rel = self.store[self.key(edge_attr)]
            if not rel.materialized:
                if rel.layout != EdgeLayout.COO.value:
                    return None
            if rel.layout == EdgeLayout.COO.value:
                self.__get_edge_coo_from_database(self.key(edge_attr))
            return rel.edge_index
        else:
            return None

    def _remove_edge_index(self, edge_attr: EdgeAttr) -> bool:
        key = self.key(edge_attr)
        if key in self.store:
            del self.store[key]

    def get_all_edge_attrs(self) -> List[EdgeAttr]:
        return [EdgeAttr(rel.edge_type, rel.layout, rel.is_sorted, rel.size) for rel in self.store.values()]

    def __get_edge_coo_from_database(self, key):
        if not self.connection:
            self.connection = Connection(self.db, self.num_threads)

        rel = self.store[key]
        if rel.layout != EdgeLayout.COO.value:
            raise ValueError("Only COO layout is supported")
        edge_type = rel.edge_type
        num_edges = self.connection._connection.get_num_rels(edge_type[1])
        result = np.zeros(2 * num_edges, dtype=np.int64)
        self.connection._connection.get_all_edges_for_torch_geometric(
            result, edge_type[0], edge_type[1], edge_type[2], REL_BATCH_SIZE)
        edge_list = torch.from_numpy(result)
        edge_list = edge_list.reshape((2, edge_list.shape[0] // 2))
        rel.edge_index = edge_list
        rel.materialized = True

    def __populate_edge_attrs(self):
        if not self.connection:
            self.connection = Connection(self.db, self.num_threads)
        rel_tables = self.connection._get_rel_table_names()
        for rel_table in rel_tables:
            edge_type = (rel_table['src'], rel_table['name'], rel_table['dst'])
            size = self.__get_size(edge_type)
            rel = Rel(edge_type, EdgeLayout.COO.value,
                      False, size, None)
            self.store[self.key(
                EdgeAttr(edge_type, EdgeLayout.COO, False))] = rel

    def __get_size(self, edge_type):
        src_count = self.connection.execute(
            "MATCH (a:%s) RETURN COUNT(*)" % edge_type[0]).get_next()[0]
        dst_count = self.connection.execute(
            "MATCH (a:%s) RETURN COUNT(*)" % edge_type[-1]).get_next()[0]
        size = (src_count, dst_count)
        return size

    def __get_edges_count(self, edge_type):
        query = "MATCH (a:%s)-[%s]->(b:%s) RETURN COUNT(*)" % edge_type
        return self.connection.execute(query).get_next()[0]
