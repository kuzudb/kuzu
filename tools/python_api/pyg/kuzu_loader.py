import os, time
import kuzu

def load_csv(conn):
    conn.execute('CREATE NODE TABLE N (ID INT64, F1 INT64, F2 INT64, F3 INT64, F4 INT64, F5 INT64, PRIMARY KEY(ID));')
    conn.execute('COPY N FROM "./data/node.csv";')
    conn.execute('CREATE REL TABLE E (FROM N TO N);')
    conn.execute('COPY E FROM "./data/edge.csv";')

import numpy as np

import torch
from torch.utils.data import DataLoader
from torch_geometric.data import Data

batch_size= 50000
shuffle = False
num_workers = 0
num_epoch = 1

class KuzuRandomLoadLoader(DataLoader):

    def __init__(
        self,
        num_nodes: int,
        num_node_features: int,
        batch_size: int,
        shuffle: bool,
        num_workers: int,
        conn,
    ):
        self.num_nodes = num_nodes
        self.num_node_features = num_node_features
        self.conn = conn
        super().__init__(range(self.num_nodes), batch_size=batch_size, shuffle=shuffle, collate_fn=self.collate_fn, num_workers=num_workers)

    def collate_fn(self, index):
        firstIdx = index[0]
        lastIdx = index[-1]
        data = Data()
        # fetch node feature
        node_feature_result = self.conn.execute(str.format(
            'MATCH (a:N) WHERE a.ID >={} AND a.ID<={} RETURN a.ID, a.F1, a.F2, a.F3, a.F4, a.F5', 
            firstIdx, lastIdx))
        node_feature_array = np.empty((self.batch_size, self.num_node_features)) # dim is known beforehand
        row_idx = 0
        pk_to_row_idx = {}
        while node_feature_result.hasNext():
            row = node_feature_result.getNext()
            for i in range(len(row)):
                node_feature_array[row_idx, i] = row[i]
            pk_to_row_idx[row[0]] = row_idx
            row_idx += 1
        data.x = torch.tensor(node_feature_array, dtype=torch.int64)
        node_feature_result.close()
        
        # fetch edge idx
        edge_idx_result = self.conn.execute(str.format(
            'MATCH (a:N)-[:E]->(b:N) WHERE a.ID >={} AND a.ID<={} AND b.ID>={} AND b.ID<={} RETURN a.ID, b.ID', 
            firstIdx, lastIdx, firstIdx, lastIdx))
        # TODO: we should know the result size to pre allocate
        src_array = []
        dst_array = []
        while edge_idx_result.hasNext():
            row = edge_idx_result.getNext()
            src_array.append(pk_to_row_idx[row[0]])
            dst_array.append(pk_to_row_idx[row[1]]) 
        src_tenstor = torch.tensor(src_array, dtype=torch.long)
        dst_tensor = torch.tensor(dst_array, dtype=torch.long)
        data.edge_index = torch.stack([src_tenstor, dst_tensor], dim=0)
        edge_idx_result.close()
        return data

def load(loader):
    start_time = time.time()
    counter = 0
    for batch in loader:
        counter += batch_size
        if counter % 10000 == 0:
            print(counter)
    end_time = time.time()
    return end_time - start_time

if __name__ == '__main__':
    db_path = './kuzu_db'
    # inject data
    load_from_csv = not os.path.exists(db_path)
    db = kuzu.database(db_path, 4 * 1024 * 1024 * 1024)
    conn = kuzu.connection(db)
    if load_from_csv:
        load_csv(conn)
    # query statictics
    result = conn.execute('MATCH (n:N) RETURN COUNT(*)')
    num_nodes = result.getNext()[0]
    result.close()

    result = conn.execute('MATCH (n:N) RETURN n LIMIT 1')
    num_node_features = len(result.getNext())
    result.close()
    
    loader = KuzuRandomLoadLoader(num_nodes=num_nodes, num_node_features=num_node_features, batch_size=batch_size, shuffle=shuffle, num_workers=num_workers, conn=conn)
    time_array = np.empty(num_epoch)
    for epoch in range(num_epoch):
        time_array[epoch] = load(loader=loader)
        print(str.format('Epoch: {}', epoch))
    print(str.format('Min: {:.2f}, Max: {:.2f}, Mean: {:.2f}, Std: {:.2f}', time_array.min(), time_array.max(), time_array.mean(), time_array.std()))
    

