from __future__ import annotations

import warnings

import ground_truth
import torch
from type_aliases import ConnDB


def test_to_torch_geometric_nodes_only(conn_db_readonly: ConnDB) -> None:
    conn, _ = conn_db_readonly
    query = "MATCH (p:person) return p"

    res = conn.execute(query)
    with warnings.catch_warnings(record=True) as ws:
        torch_geometric_data, pos_to_idx, unconverted_properties, _ = res.get_as_torch_geometric()
    warnings_ground_truth = {
        "Property person.courseScoresPerTerm cannot be converted to Tensor (likely due to nested list of variable length). The property is marked as unconverted.",
        "Property person.height of type FLOAT is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.u of type UUID is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.lastJobDuration of type INTERVAL is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.registerTime of type TIMESTAMP is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.birthdate of type DATE is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.fName of type STRING is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.workedHours has an inconsistent shape. The property is marked as unconverted.",
        "Property person.usedNames of type STRING is not supported by torch_geometric. The property is marked as unconverted.",
    }
    assert len(ws) == 9
    for w in ws:
        assert str(w.message) in warnings_ground_truth

    assert torch_geometric_data.ID.shape == torch.Size([8])
    assert torch_geometric_data.ID.dtype == torch.int64
    for i in range(8):
        assert ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["ID"] == torch_geometric_data.ID[i].item()

    assert torch_geometric_data.gender.shape == torch.Size([8])
    assert torch_geometric_data.gender.dtype == torch.int64
    for i in range(8):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["gender"]
            == torch_geometric_data.gender[i].item()
        )

    assert torch_geometric_data.isStudent.shape == torch.Size([8])
    assert torch_geometric_data.isStudent.dtype == torch.bool
    for i in range(8):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["isStudent"]
            == torch_geometric_data.isStudent[i].item()
        )

    assert torch_geometric_data.isWorker.shape == torch.Size([8])
    assert torch_geometric_data.isWorker.dtype == torch.bool
    for i in range(8):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["isWorker"]
            == torch_geometric_data.isWorker[i].item()
        )

    assert torch_geometric_data.age.shape == torch.Size([8])
    assert torch_geometric_data.age.dtype == torch.int64
    for i in range(8):
        assert ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["age"] == torch_geometric_data.age[i].item()

    assert torch_geometric_data.eyeSight.shape == torch.Size([8])
    assert torch_geometric_data.eyeSight.dtype == torch.float32
    for i in range(8):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["eyeSight"]
            - torch_geometric_data.eyeSight[i].item()
            < 1e-6
        )

    assert len(unconverted_properties) == 9
    assert "courseScoresPerTerm" in unconverted_properties
    for i in range(8):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["courseScoresPerTerm"]
            == unconverted_properties["courseScoresPerTerm"][i]
        )
    assert "lastJobDuration" in unconverted_properties
    for i in range(8):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["lastJobDuration"]
            == unconverted_properties["lastJobDuration"][i]
        )
    assert "registerTime" in unconverted_properties
    for i in range(8):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["registerTime"]
            == unconverted_properties["registerTime"][i]
        )
    assert "birthdate" in unconverted_properties
    for i in range(8):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["birthdate"]
            == unconverted_properties["birthdate"][i]
        )
    assert "fName" in unconverted_properties
    for i in range(8):
        assert ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["fName"] == unconverted_properties["fName"][i]
    assert "usedNames" in unconverted_properties
    for i in range(8):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["usedNames"]
            == unconverted_properties["usedNames"][i]
        )

    assert "workedHours" in unconverted_properties
    for i in range(8):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["workedHours"]
            == unconverted_properties["workedHours"][i]
        )


def test_to_torch_geometric_homogeneous_graph(conn_db_readonly: ConnDB) -> None:
    conn, _ = conn_db_readonly
    query = "MATCH (p:person)-[r:knows]->(q:person) RETURN p, r, q"

    res = conn.execute(query)
    with warnings.catch_warnings(record=True) as ws:
        torch_geometric_data, pos_to_idx, unconverted_properties, edge_properties = res.get_as_torch_geometric()
    warnings_ground_truth = {
        "Property person.courseScoresPerTerm cannot be converted to Tensor (likely due to nested list of variable length). The property is marked as unconverted.",
        "Property person.height of type FLOAT is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.u of type UUID is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.lastJobDuration of type INTERVAL is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.registerTime of type TIMESTAMP is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.birthdate of type DATE is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.fName of type STRING is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.workedHours has an inconsistent shape. The property is marked as unconverted.",
        "Property person.usedNames of type STRING is not supported by torch_geometric. The property is marked as unconverted.",
    }
    assert len(ws) == 9
    for w in ws:
        assert str(w.message) in warnings_ground_truth

    assert torch_geometric_data.ID.shape == torch.Size([7])
    assert torch_geometric_data.ID.dtype == torch.int64
    for i in range(7):
        assert ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["ID"] == torch_geometric_data.ID[i].item()

    assert torch_geometric_data.gender.shape == torch.Size([7])
    assert torch_geometric_data.gender.dtype == torch.int64
    for i in range(7):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["gender"]
            == torch_geometric_data.gender[i].item()
        )

    assert torch_geometric_data.isStudent.shape == torch.Size([7])
    assert torch_geometric_data.isStudent.dtype == torch.bool
    for i in range(7):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["isStudent"]
            == torch_geometric_data.isStudent[i].item()
        )

    assert torch_geometric_data.isWorker.shape == torch.Size([7])
    assert torch_geometric_data.isWorker.dtype == torch.bool
    for i in range(7):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["isWorker"]
            == torch_geometric_data.isWorker[i].item()
        )

    assert torch_geometric_data.age.shape == torch.Size([7])
    assert torch_geometric_data.age.dtype == torch.int64
    for i in range(7):
        assert ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["age"] == torch_geometric_data.age[i].item()

    assert torch_geometric_data.eyeSight.shape == torch.Size([7])
    assert torch_geometric_data.eyeSight.dtype == torch.float32
    for i in range(7):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["eyeSight"]
            - torch_geometric_data.eyeSight[i].item()
            < 1e-6
        )

    assert len(unconverted_properties) == 9
    assert "courseScoresPerTerm" in unconverted_properties
    for i in range(7):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["courseScoresPerTerm"]
            == unconverted_properties["courseScoresPerTerm"][i]
        )
    assert "lastJobDuration" in unconverted_properties
    for i in range(7):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["lastJobDuration"]
            == unconverted_properties["lastJobDuration"][i]
        )
    assert "registerTime" in unconverted_properties
    for i in range(7):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["registerTime"]
            == unconverted_properties["registerTime"][i]
        )
    assert "birthdate" in unconverted_properties
    for i in range(7):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["birthdate"]
            == unconverted_properties["birthdate"][i]
        )
    assert "fName" in unconverted_properties
    for i in range(7):
        assert ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["fName"] == unconverted_properties["fName"][i]
    assert "usedNames" in unconverted_properties
    for i in range(7):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["usedNames"]
            == unconverted_properties["usedNames"][i]
        )

    assert "workedHours" in unconverted_properties
    for i in range(7):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]["workedHours"]
            == unconverted_properties["workedHours"][i]
        )

    assert torch_geometric_data.edge_index.shape == torch.Size([2, 14])
    for i in range(14):
        src, dst = torch_geometric_data.edge_index[0][i].item(), torch_geometric_data.edge_index[1][i].item()
        assert src in pos_to_idx
        assert dst in pos_to_idx
        assert src != dst
        assert pos_to_idx[dst] in ground_truth.TINY_SNB_KNOWS_GROUND_TRUTH[pos_to_idx[src]]

    assert len(edge_properties) == 8
    assert "date" in edge_properties
    assert "meetTime" in edge_properties
    assert "validInterval" in edge_properties
    assert "comments" in edge_properties
    assert "summary" in edge_properties
    assert "notes" in edge_properties

    for i in range(14):
        src, dst = torch_geometric_data.edge_index[0][i].item(), torch_geometric_data.edge_index[1][i].item()
        orginal_src = pos_to_idx[src]
        orginal_dst = pos_to_idx[dst]
        assert (orginal_src, orginal_dst) in ground_truth.TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH
        assert (
            ground_truth.TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH[orginal_src, orginal_dst]["date"]
            == edge_properties["date"][i]
        )
        assert (
            ground_truth.TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH[orginal_src, orginal_dst]["meetTime"]
            == edge_properties["meetTime"][i]
        )
        assert (
            ground_truth.TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH[orginal_src, orginal_dst]["validInterval"]
            == edge_properties["validInterval"][i]
        )
        assert (
            ground_truth.TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH[orginal_src, orginal_dst]["comments"]
            == edge_properties["comments"][i]
        )
        assert (
            ground_truth.TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH[orginal_src, orginal_dst]["summary"]
            == edge_properties["summary"][i]
        )
        assert (
            ground_truth.TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH[orginal_src, orginal_dst]["notes"]
            == edge_properties["notes"][i]
        )
        assert (
            ground_truth.TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH[orginal_src, orginal_dst]["someMap"]
            == edge_properties["someMap"][i]
        )


def test_to_torch_geometric_heterogeneous_graph(conn_db_readonly: ConnDB) -> None:
    conn, _ = conn_db_readonly
    query = "MATCH (p:person)-[r1:knows]->(q:person)-[r2:workAt]->(o:organisation) RETURN p, q, o, r1, r2"

    res = conn.execute(query)
    with warnings.catch_warnings(record=True) as ws:
        torch_geometric_data, pos_to_idx, unconverted_properties, edge_properties = res.get_as_torch_geometric()

    assert len(ws) == 13
    warnings_ground_truth = {
        "Property organisation.name of type STRING is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.height of type FLOAT is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.u of type UUID is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.courseScoresPerTerm cannot be converted to Tensor (likely due to nested list of variable length). The property is marked as unconverted.",
        "Property person.lastJobDuration of type INTERVAL is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.registerTime of type TIMESTAMP is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.birthdate of type DATE is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.fName of type STRING is not supported by torch_geometric. The property is marked as unconverted.",
        "Property organisation.history of type STRING is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.usedNames of type STRING is not supported by torch_geometric. The property is marked as unconverted.",
        "Property organisation.licenseValidInterval of type INTERVAL is not supported by torch_geometric. The property is marked as unconverted.",
        "Property organisation.state of type STRUCT(revenue INT16, location STRING is not supported by torch_geometric. The property is marked as unconverted.",
        "Property organisation.info of type UNION(price FLOAT, movein DATE, note STRING) is not supported by torch_geometric. The property is marked as unconverted.",
    }

    for w in ws:
        assert str(w.message) in warnings_ground_truth

    assert torch_geometric_data["person"].ID.shape == torch.Size([4])
    assert torch_geometric_data["person"].ID.dtype == torch.int64
    for i in range(4):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx["person"][i]]["ID"]
            == torch_geometric_data["person"].ID[i].item()
        )

    assert torch_geometric_data["person"].gender.shape == torch.Size([4])
    assert torch_geometric_data["person"].gender.dtype == torch.int64
    for i in range(4):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx["person"][i]]["gender"]
            == torch_geometric_data["person"].gender[i].item()
        )

    assert torch_geometric_data["person"].isStudent.shape == torch.Size([4])
    assert torch_geometric_data["person"].isStudent.dtype == torch.bool
    for i in range(4):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx["person"][i]]["isStudent"]
            == torch_geometric_data["person"].isStudent[i].item()
        )

    assert torch_geometric_data["person"].isWorker.shape == torch.Size([4])
    assert torch_geometric_data["person"].isWorker.dtype == torch.bool
    for i in range(4):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx["person"][i]]["isWorker"]
            == torch_geometric_data["person"].isWorker[i].item()
        )

    assert torch_geometric_data["person"].age.shape == torch.Size([4])
    assert torch_geometric_data["person"].age.dtype == torch.int64
    for i in range(4):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx["person"][i]]["age"]
            == torch_geometric_data["person"].age[i].item()
        )

    assert torch_geometric_data["person"].eyeSight.shape == torch.Size([4])
    assert torch_geometric_data["person"].eyeSight.dtype == torch.float32
    for i in range(4):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx["person"][i]]["eyeSight"]
            - torch_geometric_data["person"].eyeSight[i].item()
            < 1e-6
        )

    assert "person" in unconverted_properties
    assert len(unconverted_properties["person"]) == 8
    assert "courseScoresPerTerm" in unconverted_properties["person"]
    for i in range(4):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx["person"][i]]["courseScoresPerTerm"]
            == unconverted_properties["person"]["courseScoresPerTerm"][i]
        )
    assert "lastJobDuration" in unconverted_properties["person"]
    for i in range(4):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx["person"][i]]["lastJobDuration"]
            == unconverted_properties["person"]["lastJobDuration"][i]
        )
    assert "registerTime" in unconverted_properties["person"]
    for i in range(4):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx["person"][i]]["registerTime"]
            == unconverted_properties["person"]["registerTime"][i]
        )
    assert "birthdate" in unconverted_properties["person"]
    for i in range(4):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx["person"][i]]["birthdate"]
            == unconverted_properties["person"]["birthdate"][i]
        )
    assert "fName" in unconverted_properties["person"]
    for i in range(4):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx["person"][i]]["fName"]
            == unconverted_properties["person"]["fName"][i]
        )
    assert "usedNames" in unconverted_properties["person"]
    for i in range(4):
        assert (
            ground_truth.TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx["person"][i]]["usedNames"]
            == unconverted_properties["person"]["usedNames"][i]
        )

    assert torch_geometric_data["person", "person"].edge_index.shape == torch.Size([2, 6])
    for i in range(3):
        src, dst = (
            torch_geometric_data["person", "person"].edge_index[0][i].item(),
            torch_geometric_data["person", "person"].edge_index[1][i].item(),
        )
        assert src in pos_to_idx["person"]
        assert dst in pos_to_idx["person"]
        assert src != dst
        assert pos_to_idx["person"][dst] in ground_truth.TINY_SNB_KNOWS_GROUND_TRUTH[pos_to_idx["person"][src]]

    assert len(edge_properties["person", "person"]) == 8
    assert "date" in edge_properties["person", "person"]
    assert "meetTime" in edge_properties["person", "person"]
    assert "validInterval" in edge_properties["person", "person"]
    assert "comments" in edge_properties["person", "person"]
    assert "summary" in edge_properties["person", "person"]
    assert "notes" in edge_properties["person", "person"]
    assert "_label" in edge_properties["person", "person"]
    for i in range(3):
        src, dst = (
            torch_geometric_data["person", "person"].edge_index[0][i].item(),
            torch_geometric_data["person", "person"].edge_index[1][i].item(),
        )
        original_src, original_dst = pos_to_idx["person"][src], pos_to_idx["person"][dst]
        assert (original_src, original_dst) in ground_truth.TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH
        assert (
            ground_truth.TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH[original_src, original_dst]["date"]
            == edge_properties["person", "person"]["date"][i]
        )
        assert (
            ground_truth.TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH[original_src, original_dst]["meetTime"]
            == edge_properties["person", "person"]["meetTime"][i]
        )
        assert (
            ground_truth.TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH[original_src, original_dst]["validInterval"]
            == edge_properties["person", "person"]["validInterval"][i]
        )
        assert (
            ground_truth.TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH[original_src, original_dst]["comments"]
            == edge_properties["person", "person"]["comments"][i]
        )
        assert (
            ground_truth.TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH[original_src, original_dst]["summary"]
            == edge_properties["person", "person"]["summary"][i]
        )
        assert (
            ground_truth.TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH[original_src, original_dst]["notes"]
            == edge_properties["person", "person"]["notes"][i]
        )
        assert edge_properties["person", "person"]["_label"][i] == "knows"

    assert torch_geometric_data["organisation"].ID.shape == torch.Size([2])
    assert torch_geometric_data["organisation"].ID.dtype == torch.int64
    for i in range(2):
        assert (
            ground_truth.TINY_SNB_ORGANISATIONS_GROUND_TRUTH[pos_to_idx["organisation"][i]]["ID"]
            == torch_geometric_data["organisation"].ID[i].item()
        )

    assert torch_geometric_data["organisation"].orgCode.shape == torch.Size([2])
    assert torch_geometric_data["organisation"].orgCode.dtype == torch.int64
    for i in range(2):
        assert (
            ground_truth.TINY_SNB_ORGANISATIONS_GROUND_TRUTH[pos_to_idx["organisation"][i]]["orgCode"]
            == torch_geometric_data["organisation"].orgCode[i].item()
        )

    assert torch_geometric_data["organisation"].mark.shape == torch.Size([2])
    assert torch_geometric_data["organisation"].mark.dtype == torch.float32
    for i in range(2):
        assert (
            ground_truth.TINY_SNB_ORGANISATIONS_GROUND_TRUTH[pos_to_idx["organisation"][i]]["mark"]
            - torch_geometric_data["organisation"].mark[i].item()
            < 1e-6
        )

    assert torch_geometric_data["organisation"].score.shape == torch.Size([2])
    assert torch_geometric_data["organisation"].score.dtype == torch.int64
    for i in range(2):
        assert (
            ground_truth.TINY_SNB_ORGANISATIONS_GROUND_TRUTH[pos_to_idx["organisation"][i]]["score"]
            - torch_geometric_data["organisation"].score[i].item()
            < 1e-6
        )

    assert torch_geometric_data["organisation"].rating.shape == torch.Size([2])
    assert torch_geometric_data["organisation"].rating.dtype == torch.float32
    for i in range(2):
        assert (
            ground_truth.TINY_SNB_ORGANISATIONS_GROUND_TRUTH[pos_to_idx["organisation"][i]]["rating"]
            - torch_geometric_data["organisation"].rating[i].item()
            < 1e-6
        )

    assert "organisation" in unconverted_properties
    assert len(unconverted_properties["organisation"]) == 5
    assert "name" in unconverted_properties["organisation"]
    for i in range(2):
        assert (
            ground_truth.TINY_SNB_ORGANISATIONS_GROUND_TRUTH[pos_to_idx["organisation"][i]]["name"]
            == unconverted_properties["organisation"]["name"][i]
        )

    assert "history" in unconverted_properties["organisation"]
    for i in range(2):
        assert (
            ground_truth.TINY_SNB_ORGANISATIONS_GROUND_TRUTH[pos_to_idx["organisation"][i]]["history"]
            == unconverted_properties["organisation"]["history"][i]
        )

    assert "licenseValidInterval" in unconverted_properties["organisation"]
    for i in range(2):
        assert (
            ground_truth.TINY_SNB_ORGANISATIONS_GROUND_TRUTH[pos_to_idx["organisation"][i]]["licenseValidInterval"]
            == unconverted_properties["organisation"]["licenseValidInterval"][i]
        )

    assert torch_geometric_data["person", "organisation"].edge_index.shape == torch.Size([2, 2])
    for i in range(2):
        src, dst = (
            torch_geometric_data["person", "organisation"].edge_index[0][i].item(),
            torch_geometric_data["person", "organisation"].edge_index[1][i].item(),
        )
        assert src in pos_to_idx["person"]
        assert dst in pos_to_idx["organisation"]
        assert src != dst
        assert (
            pos_to_idx["organisation"][dst] in ground_truth.TINY_SNB_WORKS_AT_GROUND_TRUTH[pos_to_idx["person"][src]]
        )
    assert len(edge_properties["person", "organisation"]) == 4
    assert "year" in edge_properties["person", "organisation"]
    assert "_label" in edge_properties["person", "organisation"]
    for i in range(2):
        src, dst = (
            torch_geometric_data["person", "organisation"].edge_index[0][i].item(),
            torch_geometric_data["person", "organisation"].edge_index[1][i].item(),
        )
        original_src, original_dst = pos_to_idx["person"][src], pos_to_idx["organisation"][dst]
        assert (
            ground_truth.TINY_SNB_WORKS_AT_PROPERTIES_GROUND_TRUTH[original_src, original_dst]["year"]
            == edge_properties["person", "organisation"]["year"][i]
        )
        assert edge_properties["person", "organisation"]["_label"][i] == "workAt"


def test_to_torch_geometric_multi_dimensional_lists(
    conn_db_readonly: ConnDB,
) -> None:
    conn, _ = conn_db_readonly
    query = "MATCH (t:tensor) RETURN t"

    res = conn.execute(query)
    with warnings.catch_warnings(record=True) as ws:
        torch_geometric_data, pos_to_idx, unconverted_properties, _ = res.get_as_torch_geometric()
    assert len(ws) == 1
    assert (
        str(ws[0].message)
        == "Property tensor.oneDimInt has a null value. torch_geometric does not support null values. The property is marked as unconverted."
    )

    bool_list = []
    float_list = []
    int_list = []

    for i in range(len(pos_to_idx)):
        idx = pos_to_idx[i]
        bool_list.append(ground_truth.TENSOR_LIST_GROUND_TRUTH[idx]["boolTensor"])
        float_list.append(ground_truth.TENSOR_LIST_GROUND_TRUTH[idx]["doubleTensor"])
        int_list.append(ground_truth.TENSOR_LIST_GROUND_TRUTH[idx]["intTensor"])

    bool_tensor = torch.tensor(bool_list, dtype=torch.bool)
    float_tensor = torch.tensor(float_list, dtype=torch.float32)
    int_tensor = torch.tensor(int_list, dtype=torch.int64)

    assert torch_geometric_data.ID.shape == torch.Size([len(pos_to_idx)])
    assert torch_geometric_data.ID.dtype == torch.int64
    for i in range(len(pos_to_idx)):
        assert torch_geometric_data.ID[i].item() == pos_to_idx[i]

    assert torch_geometric_data.boolTensor.shape == bool_tensor.shape
    assert torch_geometric_data.boolTensor.dtype == bool_tensor.dtype
    assert torch.all(torch_geometric_data.boolTensor == bool_tensor)

    assert torch_geometric_data.doubleTensor.shape == float_tensor.shape
    assert torch_geometric_data.doubleTensor.dtype == float_tensor.dtype
    assert torch.all(torch_geometric_data.doubleTensor == float_tensor)

    assert torch_geometric_data.intTensor.shape == int_tensor.shape
    assert torch_geometric_data.intTensor.dtype == int_tensor.dtype
    assert torch.all(torch_geometric_data.intTensor == int_tensor)

    assert len(unconverted_properties) == 1
    assert "oneDimInt" in unconverted_properties
    assert len(unconverted_properties["oneDimInt"]) == 6
    assert unconverted_properties["oneDimInt"] == [1, 2, None, None, 5, 6]


def test_to_torch_geometric_no_properties_converted(
    conn_db_readonly: ConnDB,
) -> None:
    conn, _ = conn_db_readonly
    query = "MATCH (p:personLongString)-[r:knowsLongString]->(q:personLongString) RETURN p, r, q"

    res = conn.execute(query)
    with warnings.catch_warnings(record=True) as ws:
        torch_geometric_data, pos_to_idx, unconverted_properties, _ = res.get_as_torch_geometric()
    assert len(ws) == 3
    warnings_ground_truth = {
        "Property personLongString.name of type STRING is not supported by torch_geometric. The property is marked as unconverted.",
        "Property personLongString.spouse of type STRING is not supported by torch_geometric. The property is marked as unconverted.",
        "No nodes found or all node properties are not converted.",
    }
    for w in ws:
        assert str(w.message) in warnings_ground_truth
    assert torch_geometric_data["personLongString"] == {}
    assert torch_geometric_data["personLongString", "personLongString"].edge_index.shape == torch.Size([2, 1])

    for i in range(1):
        src, dst = (
            torch_geometric_data["personLongString", "personLongString"].edge_index[0][i].item(),
            torch_geometric_data["personLongString", "personLongString"].edge_index[1][i].item(),
        )
        assert src in pos_to_idx["personLongString"]
        assert dst in pos_to_idx["personLongString"]
        assert src != dst
        assert (
            pos_to_idx["personLongString"][dst]
            in ground_truth.PERSONLONGSTRING_KNOWS_GROUND_TRUTH[pos_to_idx["personLongString"][src]]
        )

    assert len(unconverted_properties) == 1
    assert len(unconverted_properties["personLongString"]) == 2

    assert "spouse" in unconverted_properties["personLongString"]
    assert len(unconverted_properties["personLongString"]["spouse"]) == 2
    for i in range(2):
        assert (
            ground_truth.PERSONLONGSTRING_GROUND_TRUTH[pos_to_idx["personLongString"][i]]["spouse"]
            == unconverted_properties["personLongString"]["spouse"][i]
        )

    assert "name" in unconverted_properties["personLongString"]
    for i in range(2):
        assert (
            ground_truth.PERSONLONGSTRING_GROUND_TRUTH[pos_to_idx["personLongString"][i]]["name"]
            == unconverted_properties["personLongString"]["name"][i]
        )
