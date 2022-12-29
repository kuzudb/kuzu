import numpy as np

import torch

from torch_geometric.data import Data


class BaseDataGenerator:

    def __init__(self) -> None:
        # constants
        self.scale_factor = 100
        self.base_num_nodes = 1000
        self.num_node_features = 5
        self.num_edges_per_node = 20
        # inferred statistics
        self.num_nodes = self.scale_factor * self.base_num_nodes
        self.num_edges = self.num_nodes * self.num_edges_per_node

    def _generate_node_matrix(self):
        id = np.arange(0, self.num_nodes).reshape(self.num_nodes, 1)
        features = np.ones((self.num_nodes, self.num_node_features), dtype=np.int64)
        return np.concatenate([id, features], axis=1)

    def _generate_edge_matrix(self):
        src_array = np.empty(self.num_edges, dtype=np.int64)
        dst_array = np.empty(self.num_edges, dtype=np.int64)
        counter = 0
        for i in range(self.num_nodes):
            src_idx = i
            for j in range(self.num_edges_per_node):
                dst_idx = i + j if i + j < self.num_nodes else self.num_nodes - 1
                src_array[counter] = src_idx
                dst_array[counter] = dst_idx
                counter += 1

        return src_array, dst_array


class PyGDataGenerator(BaseDataGenerator):

    def __init__(self) -> None:
        super().__init__()

    def generate(self):
        data = Data()
        data.x = torch.tensor(self._generate_node_matrix(), dtype=torch.int64)
        src, dst = self._generate_edge_matrix()
        src_tenstor = torch.tensor(src, dtype=torch.long)
        dst_tensor = torch.tensor(dst, dtype=torch.long)
        data.edge_index = torch.stack([src_tenstor, dst_tensor], dim=0)
        return data


class CSVDataGenerator(BaseDataGenerator):

    def __init__(self) -> None:
        super().__init__()
        self.root = './data/'
        self.node_path = self.root + 'node.csv'
        self.edge_path = self.root + 'edge.csv'

    def generate(self):
        node_matrix = self._generate_node_matrix()
        np.savetxt(self.node_path, node_matrix, delimiter=',', fmt="%d")
        src, dst = self._generate_edge_matrix()
        edge_matrix = np.column_stack([src, dst])
        np.savetxt(self.edge_path, edge_matrix, delimiter=',', fmt="%d")


if __name__ == '__main__':
    csv_generator = CSVDataGenerator()
    csv_generator.generate()
    # pyg_generator = PyGDataGenerator()
    # data = pyg_generator.generate()
    # print(data.x)
