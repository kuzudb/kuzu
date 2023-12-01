import sys
sys.path.append('/home/lc/Developer/guodong/kuzu/tools/python_api/build')
import kuzu

DB_PATH = '/home/lc/Developer/kuzu_ogb_mag'
db = kuzu.Database(DB_PATH, (10*1024**3))
conn = kuzu.Connection(db)
t = conn.get_all_edges_for_torch_geometric(10792672, "paper", "cites", "paper", 1000000)
