import torch_geometric.transforms as T
from torch_geometric.datasets import OGB_MAG
import torch
from torch_geometric.loader import NeighborLoader
import time
import sys
import multiprocessing

if __name__ == "__main__":
    multiprocessing.freeze_support()

    if len(sys.argv) != 6:
        print(
            "Usage: %s <db_path> <num_workers> <batch_size> <warmup> <batches>" % sys.argv[0])
        sys.exit(1)

    data_path = sys.argv[1]
    num_workers = int(sys.argv[2])
    batch_size = int(sys.argv[3])
    warmup = int(sys.argv[4])
    batches = int(sys.argv[5])

    dataset = OGB_MAG(root=data_path, preprocess='metapath2vec', transform=T.ToUndirected())
    data = dataset[0]

    loader_pyg = NeighborLoader(
        data=data,
        num_neighbors={key: [30] * 2 for key in data.edge_types},
        # Use a batch size of 128 for sampling training nodes of type paper
        batch_size=batch_size,
        input_nodes=('paper', torch.arange(0, 736389)),
        input_time=None,
        num_workers=num_workers,
    )

    i = 0
    for b in loader_pyg:
        i += 1
        if i == warmup:
            break

    start = time.time()
    i = 0
    for b in loader_pyg:
        i += 1
        if i == batches:
            break
    end = time.time() - start

    batch_time = end / batches

    print("pyg\t%d\t%d\t%f" % (num_workers, batch_size, batch_time))
