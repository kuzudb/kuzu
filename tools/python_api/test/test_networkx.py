from __future__ import annotations

import datetime
from typing import Any

from pandas import Timedelta, Timestamp
from type_aliases import ConnDB


def test_to_networkx_node(conn_db_readonly: ConnDB) -> None:
    conn, _ = conn_db_readonly
    query = "MATCH (p:person) return p"

    res = conn.execute(query)
    nx_graph = res.get_as_networkx()
    nodes = list(nx_graph.nodes(data=True))
    assert len(nodes) == 8

    ground_truth: dict[str, list[Any]] = {
        "ID": [0, 2, 3, 5, 7, 8, 9, 10],
        "fName": [
            "Alice",
            "Bob",
            "Carol",
            "Dan",
            "Elizabeth",
            "Farooq",
            "Greg",
            "Hubert Blaine Wolfeschlegelsteinhausenbergerdorff",
        ],
        "gender": [1, 2, 1, 2, 1, 2, 2, 2],
        "isStudent": [True, True, False, False, False, True, False, False],
        "eyeSight": [5.0, 5.1, 5.0, 4.8, 4.7, 4.5, 4.9, 4.9],
        "birthdate": [
            datetime.date(1900, 1, 1),
            datetime.date(1900, 1, 1),
            datetime.date(1940, 6, 22),
            datetime.date(1950, 7, 23),
            datetime.date(1980, 10, 26),
            datetime.date(1980, 10, 26),
            datetime.date(1980, 10, 26),
            datetime.date(1990, 11, 27),
        ],
        "registerTime": [
            Timestamp("2011-08-20 11:25:30"),
            Timestamp("2008-11-03 15:25:30.000526"),
            Timestamp("1911-08-20 02:32:21"),
            Timestamp("2031-11-30 12:25:30"),
            Timestamp("1976-12-23 11:21:42"),
            Timestamp("1972-07-31 13:22:30.678559"),
            Timestamp("1976-12-23 04:41:42"),
            Timestamp("2023-02-21 13:25:30"),
        ],
        "lastJobDuration": [
            Timedelta("1082 days 13:02:00"),
            Timedelta("3750 days 13:00:00.000024"),
            Timedelta("2 days 00:24:11"),
            Timedelta("3750 days 13:00:00.000024"),
            Timedelta("2 days 00:24:11"),
            Timedelta("0 days 00:18:00.024000"),
            Timedelta("3750 days 13:00:00.000024"),
            Timedelta("1082 days 13:02:00"),
        ],
        "workedHours": [
            [10, 5],
            [12, 8],
            [4, 5],
            [1, 9],
            [2],
            [3, 4, 5, 6, 7],
            [1],
            [10, 11, 12, 3, 4, 5, 6, 7],
        ],
        "usedNames": [
            ["Aida"],
            ["Bobby"],
            ["Carmen", "Fred"],
            ["Wolfeschlegelstein", "Daniel"],
            ["Ein"],
            ["Fesdwe"],
            ["Grad"],
            ["Ad", "De", "Hi", "Kye", "Orlan"],
        ],
        "courseScoresPerTerm": [
            [[10, 8], [6, 7, 8]],
            [[8, 9], [9, 10]],
            [[8, 10]],
            [[7, 4], [8, 8], [9]],
            [[6], [7], [8]],
            [[8]],
            [[10]],
            [[7], [10], [6, 7]],
        ],
        "grades": [
            [96, 54, 86, 92],
            [98, 42, 93, 88],
            [91, 75, 21, 95],
            [76, 88, 99, 89],
            [96, 59, 65, 88],
            [80, 78, 34, 83],
            [43, 83, 67, 43],
            [77, 64, 100, 54],
        ],
        "_label": [
            "person",
            "person",
            "person",
            "person",
            "person",
            "person",
            "person",
            "person",
        ],
    }
    for i in range(len(nodes)):
        node_id, node = nodes[i]
        assert node_id == f"{node['_label']}_{node['ID']}"
        for key in ground_truth:
            assert node[key] == ground_truth[key][i]


def test_networkx_undirected(conn_db_readonly: ConnDB) -> None:
    conn, _ = conn_db_readonly
    res = conn.execute("MATCH (p1:person)-[r:knows]->(p2:person) WHERE p1.ID <= 3 RETURN p1, r, p2")

    nx_graph = res.get_as_networkx(directed=False)
    assert not nx_graph.is_directed()

    nodes = list(nx_graph.nodes(data=True))
    assert len(nodes) == 4

    edges = list(nx_graph.edges(data=True))

    ground_truth_p: dict[str, list[Any]] = {
        "ID": [0, 2, 3, 5, 7, 8, 9, 10],
        "fName": [
            "Alice",
            "Bob",
            "Carol",
            "Dan",
            "Elizabeth",
            "Farooq",
            "Greg",
            "Hubert Blaine Wolfeschlegelsteinhausenbergerdorff",
        ],
        "gender": [1, 2, 1, 2, 1, 2, 2, 2],
        "isStudent": [True, True, False, False, False, True, False, False],
        "eyeSight": [5.0, 5.1, 5.0, 4.8, 4.7, 4.5, 4.9, 4.9],
        "birthdate": [
            datetime.date(1900, 1, 1),
            datetime.date(1900, 1, 1),
            datetime.date(1940, 6, 22),
            datetime.date(1950, 7, 23),
            datetime.date(1980, 10, 26),
            datetime.date(1980, 10, 26),
            datetime.date(1980, 10, 26),
            datetime.date(1990, 11, 27),
        ],
        "registerTime": [
            Timestamp("2011-08-20 11:25:30"),
            Timestamp("2008-11-03 15:25:30.000526"),
            Timestamp("1911-08-20 02:32:21"),
            Timestamp("2031-11-30 12:25:30"),
            Timestamp("1976-12-23 11:21:42"),
            Timestamp("1972-07-31 13:22:30.678559"),
            Timestamp("1976-12-23 04:41:42"),
            Timestamp("2023-02-21 13:25:30"),
        ],
        "lastJobDuration": [
            Timedelta("1082 days 13:02:00"),
            Timedelta("3750 days 13:00:00.000024"),
            Timedelta("2 days 00:24:11"),
            Timedelta("3750 days 13:00:00.000024"),
            Timedelta("2 days 00:24:11"),
            Timedelta("0 days 00:18:00.024000"),
            Timedelta("3750 days 13:00:00.000024"),
            Timedelta("1082 days 13:02:00"),
        ],
        "workedHours": [
            [10, 5],
            [12, 8],
            [4, 5],
            [1, 9],
            [2],
            [3, 4, 5, 6, 7],
            [1],
            [10, 11, 12, 3, 4, 5, 6, 7],
        ],
        "usedNames": [
            ["Aida"],
            ["Bobby"],
            ["Carmen", "Fred"],
            ["Wolfeschlegelstein", "Daniel"],
            ["Ein"],
            ["Fesdwe"],
            ["Grad"],
            ["Ad", "De", "Hi", "Kye", "Orlan"],
        ],
        "courseScoresPerTerm": [
            [[10, 8], [6, 7, 8]],
            [[8, 9], [9, 10]],
            [[8, 10]],
            [[7, 4], [8, 8], [9]],
            [[6], [7], [8]],
            [[8]],
            [[10]],
            [[7], [10], [6, 7]],
        ],
        "grades": [
            [96, 54, 86, 92],
            [98, 42, 93, 88],
            [91, 75, 21, 95],
            [76, 88, 99, 89],
            [96, 59, 65, 88],
            [80, 78, 34, 83],
            [43, 83, 67, 43],
            [77, 64, 100, 54],
        ],
        "_label": [
            "person",
            "person",
            "person",
            "person",
            "person",
            "person",
            "person",
            "person",
        ],
    }
    for node_id, node in nodes:
        assert node_id == f"{node['_label']}_{node['ID']}"

    for _, node in nodes:
        found = False
        for i in range(len(ground_truth_p["ID"])):
            if node["ID"] != ground_truth_p["ID"][i]:
                continue
            found = True
            for key in ground_truth_p:
                assert node[key] == ground_truth_p[key][i]
        assert found

    assert len(edges) == 9
    # This should be a complete graph, so we check if an edge exists between
    # every pair of nodes and that there are no self-loops
    for i in range(len(nodes)):
        assert not nx_graph.has_edge(nodes[i][0], nodes[i][0])
        for j in range(i + 1, len(nodes)):
            assert nx_graph.has_edge(nodes[i][0], nodes[j][0])


def test_networkx_directed(conn_db_readonly: ConnDB) -> None:
    conn, _ = conn_db_readonly
    res = conn.execute("MATCH (p:person)-[r:workAt]->(o:organisation) RETURN p, r, o")

    nx_graph = res.get_as_networkx(directed=True)
    assert nx_graph.is_directed()

    nodes = list(nx_graph.nodes(data=True))
    assert len(nodes) == 5

    edges = list(nx_graph.edges(data=True))

    ground_truth_p: dict[str, list[Any]] = {
        "ID": [3, 5, 7],
        "fName": ["Carol", "Dan", "Elizabeth"],
        "gender": [1, 2, 1],
        "isStudent": [False, False, False],
        "eyeSight": [5.0, 4.8, 4.7],
        "birthdate": [
            datetime.date(1940, 6, 22),
            datetime.date(1950, 7, 23),
            datetime.date(1980, 10, 26),
        ],
        "registerTime": [
            Timestamp("1911-08-20 02:32:21"),
            Timestamp("2031-11-30 12:25:30"),
            Timestamp("1976-12-23 11:21:42"),
        ],
        "lastJobDuration": [
            Timedelta("48 hours 24 minutes 11 seconds"),
            Timedelta("3750 days 13:00:00.000024"),
            Timedelta("2 days 00:24:11"),
        ],
        "workedHours": [[4, 5], [1, 9], [2]],
        "usedNames": [["Carmen", "Fred"], ["Wolfeschlegelstein", "Daniel"], ["Ein"]],
        "courseScoresPerTerm": [[[8, 10]], [[7, 4], [8, 8], [9]], [[6], [7], [8]]],
        "grades": [[91, 75, 21, 95], [76, 88, 99, 89], [96, 59, 65, 88]],
        "_label": ["person", "person", "person"],
    }

    for node_id, node in nodes:
        assert node_id == f"{node['_label']}_{node['ID']}"

    for _, node in nodes:
        if "person" not in node:
            continue
        found = False
        for i in range(len(ground_truth_p["ID"])):
            if node["ID"] != ground_truth_p["ID"][i]:
                continue
            found = True
            for key in ground_truth_p:
                assert node[key] == ground_truth_p[key][i]
        assert found

    ground_truth_o: dict[str, list[Any]] = {
        "ID": [4, 6],
        "name": ["CsWork", "DEsWork"],
        "orgCode": [934, 824],
        "mark": [4.1, 4.1],
        "score": [-100, 7],
        "history": ["2 years 4 days 10 hours", "2 years 4 hours 22 us 34 minutes"],
        "licenseValidInterval": [
            Timedelta(days=9414),
            Timedelta(days=3, seconds=36000, microseconds=100000),
        ],
        "rating": [0.78, 0.52],
        "_label": ["organisation", "organisation"],
    }

    for _, node in nodes:
        if "organisation" not in node:
            continue
        found = False
        for i in range(len(ground_truth_o["ID"])):
            if node["ID"] != ground_truth_o["ID"][i]:
                continue
            found = True
            for key in ground_truth_o:
                assert node[key] == ground_truth_o[key][i]
        assert found

    nodes_dict = dict(nx_graph.nodes(data=True))
    edges = list(nx_graph.edges(data=True))
    assert len(edges) == 3

    years_ground_truth = [2010, 2015, 2015]
    for src, dst, edge in edges:
        assert nodes_dict[dst]["_label"] == "organisation"
        assert nodes_dict[dst]["ID"] in [4, 6]
        assert nodes_dict[src]["_label"] == "person"
        assert nodes_dict[src]["ID"] in [3, 5, 7]
        assert edge["year"] in years_ground_truth

        # If the edge is found, remove it from ground truth so we can check
        # that all edges were found and no extra edges were found
        del years_ground_truth[years_ground_truth.index(edge["year"])]
        del nodes_dict[src]

    assert len(years_ground_truth) == 0
    assert len(nodes_dict) == 2  # Only the organisation node should be left


def test_networkx_optional_match(conn_db_readonly: ConnDB) -> None:
    conn, _ = conn_db_readonly
    res = conn.execute(
        """
                       MATCH (u:User)
                       OPTIONAL MATCH (u)-[f:Follows]->(u1:User)
                       RETURN u, f, u1;
                       """
    )

    nx_graph = res.get_as_networkx(directed=True)
    assert nx_graph.is_directed()

    ground_truth_users = [
        {
            "name": "Adam",
            "age": 30,
        },
        {
            "name": "Karissa",
            "age": 40,
        },
        {
            "name": "Zhang",
            "age": 50,
        },
        {
            "name": "Noura",
            "age": 25,
        },
    ]

    ground_truth_follows = [
        {
            "src": "Adam",
            "dst": "Karissa",
            "since": 2020,
        },
        {
            "src": "Adam",
            "dst": "Zhang",
            "since": 2020,
        },
        {
            "src": "Karissa",
            "dst": "Zhang",
            "since": 2021,
        },
        {
            "src": "Zhang",
            "dst": "Noura",
            "since": 2022,
        },
    ]

    nodes = list(nx_graph.nodes(data=True))
    assert len(nodes) == 4
    for node_id, node in nodes:
        assert node_id == f"User_{node['name']}"
        for user in ground_truth_users:
            if user["name"] == node["name"]:
                for key in user:
                    assert node[key] == user[key]
                assert node["User"]

    for src, dst, data in nx_graph.edges(data=True):
        assert src in nx_graph.nodes
        assert dst in nx_graph.nodes
        src_node = nx_graph.nodes[src]
        dst_node = nx_graph.nodes[dst]
        src_name = src_node["name"]
        dst_name = dst_node["name"]
        for follows in ground_truth_follows:
            if follows["src"] == src_name and follows["dst"] == dst_name:
                assert data["since"] == follows["since"]
                break
        else:
            msg = f"Edge {src_name} -> {dst_name} not found in ground truth"
            raise AssertionError(msg)

    edges = list(nx_graph.edges(data=True))
    assert len(edges) == 4
