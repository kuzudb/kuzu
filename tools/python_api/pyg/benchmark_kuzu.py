import multiprocessing
from torch_geometric.loader import NeighborLoader
import time
import torch
import sys
sys.path.append('../build')
import kuzu

if __name__ == "__main__":
    multiprocessing.freeze_support()

    if len(sys.argv) != 6:
        print(
            "Usage: %s <db_path> <num_workers> <batch_size> <warmup> <batches>" % sys.argv[0])
        sys.exit(1)
    

    db_path = sys.argv[1]
    num_workers = int(sys.argv[2])
    batch_size = int(sys.argv[3])
    warmup = int(sys.argv[4])
    batches = int(sys.argv[5])
    db = kuzu.Database(db_path, buffer_pool_size=1024**3)
    fs, gs = db.get_torch_geometric_remote_backend()

    loader_kuzu = NeighborLoader(
        data=(fs, gs),
        num_neighbors={key.edge_type: [30] *
                       2 for key in gs.get_all_edge_attrs()},
        batch_size=batch_size,
        input_nodes=('paper', torch.arange(0, 736389)),
        input_time=None,
        num_workers=num_workers,
    )

    i = 0
    for b in loader_kuzu:
        i += 1
        if i == warmup:
            break

    start = time.time()
    i = 0
    for b in loader_kuzu:
        i += 1
        if i == batches:
            break
    end = time.time() - start

    batch_time = end / batches

    print("kuzu\t%d\t%d\t%f" % (num_workers, batch_size, batch_time))
