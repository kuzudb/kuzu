from __future__ import annotations

TINY_SNB_KNOWS_GROUND_TRUTH = {
    0: [2, 3, 5],
    2: [0, 3, 5],
    3: [0, 2, 5],
    5: [0, 2, 3],
    7: [8, 9],
}

TINY_SNB_PERSON_IDS_GROUND_TRUTH = [0, 2, 3, 5, 7, 8, 9, 10]

# TODO: FIX-ME. Re-enable the tests when StorageDriver is fixed.
# def test_remote_backend_graph_store(conn_db_readonly: ConnDB) -> None:
#     _, db = conn_db_readonly
#     fs, gs = db.get_torch_geometric_remote_backend()
#     edge_idx = gs.get_edge_index(("person", "knows", "person"), layout="coo", is_sorted=False)
#     assert edge_idx.shape == torch.Size([2, 14])
#     for i in range(14):
#         src = edge_idx[0, i].item()
#         src_id = fs["person", "ID", src].item()
#         dst = edge_idx[1, i].item()
#         dst_id = fs["person", "ID", dst].item()
#         assert src_id in TINY_SNB_KNOWS_GROUND_TRUTH
#         assert dst_id in TINY_SNB_KNOWS_GROUND_TRUTH[src_id]
#
#
# def test_remote_backend_feature_store_idx(conn_db_readonly: ConnDB) -> None:
#     _, db = conn_db_readonly
#     fs, _ = db.get_torch_geometric_remote_backend()
#     id_set = set(TINY_SNB_PERSON_IDS_GROUND_TRUTH)
#     index_to_id = {}
#     # Individual index access
#     for i in range(len(id_set)):
#         assert fs["person", "ID", i].item() in id_set
#         id_set.remove(fs["person", "ID", i].item())
#         index_to_id[i] = fs["person", "ID", i].item()
#
#     # Range index access
#     for i in range(len(index_to_id) - 3):
#         ids = fs["person", "ID", i : i + 3]
#         for j in range(3):
#             assert ids[j].item() == index_to_id[i + j]
#
#     # Multiple index access
#     indicies = random.sample(range(len(index_to_id)), 4)
#     ids = fs["person", "ID", indicies]
#     for i in range(4):
#         idx = indicies[i]
#         assert ids[i].item() == index_to_id[idx]
#
#     # No index access
#     ids = fs["person", "ID", None]
#     assert len(ids) == len(index_to_id)
#     for i in range(len(ids)):
#         assert ids[i].item() == index_to_id[i]
#
#
# def test_remote_backend_feature_store_types_1d(conn_db_readonly: ConnDB) -> None:
#     _, db = conn_db_readonly
#     fs, _ = db.get_torch_geometric_remote_backend()
#     for i in range(3):
#         assert fs["npyoned", "i64", i].item() - i == 1
#         assert fs["npyoned", "i32", i].item() - i == 1
#         assert fs["npyoned", "i16", i].item() - i == 1
#         assert fs["npyoned", "f64", i].item() - i - 1 < 1e-6
#         assert fs["npyoned", "f32", i].item() - i - 1 < 1e-6
#
#
# def test_remote_backend_feature_store_types_2d(conn_db_readonly: ConnDB) -> None:
#     _, db = conn_db_readonly
#     fs, _ = db.get_torch_geometric_remote_backend()
#     for i in range(3):
#         i64 = fs["npytwod", "i64", i]
#         assert i64.shape == torch.Size([1, 3])
#         base_number = (i * 3) + 1
#         for j in range(3):
#             assert i64[0, j].item() == base_number + j
#         i32 = fs["npytwod", "i32", i]
#         assert i32.shape == torch.Size([1, 3])
#         for j in range(3):
#             assert i32[0, j].item() == base_number + j
#         i16 = fs["npytwod", "i16", i]
#         assert i16.shape == torch.Size([1, 3])
#         for j in range(3):
#             assert i16[0, j].item() == base_number + j
#         f64 = fs["npytwod", "f64", i]
#         assert f64.shape == torch.Size([1, 3])
#         for j in range(3):
#             assert f64[0, j].item() - (base_number + j) - 1 < 1e-6
#         f32 = fs["npytwod", "f32", i]
#         assert f32.shape == torch.Size([1, 3])
#         for j in range(3):
#             assert f32[0, j].item() - (base_number + j) - 1 < 1e-6
#
#
# def test_remote_backend_20k(conn_db_readwrite: ConnDB) -> None:
#     _, db = conn_db_readwrite
#     conn = kuzu.Connection(db, num_threads=1)
#     conn.execute("CREATE NODE TABLE npy20k (id INT64,f32 FLOAT[10],PRIMARY KEY(id));")
#     conn.execute(
#         f"""
#         COPY npy20k FROM (
#           "{KUZU_ROOT}/dataset/npy-20k/id_int64.npy",
#           "{KUZU_ROOT}/dataset/npy-20k/two_dim_float.npy") BY COLUMN;
#         """
#     )
#     del conn
#     fs, _ = db.get_torch_geometric_remote_backend(8)
#     for i in range(20000):
#         assert fs["npy20k", "id", i].item() == i
