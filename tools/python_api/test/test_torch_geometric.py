import datetime
import torch
import warnings

TINY_SNB_PERSONS_GROUND_TRUTH = {0: {'ID': 0,
                                     'fName': 'Alice',
                                     'gender': 1,
                                     'isStudent': True,
                                     'isWorker': False,
                                     'age': 35,
                                     'eyeSight': 5.0,
                                     'birthdate': datetime.date(1900, 1, 1),
                                     'registerTime': datetime.datetime(2011, 8, 20, 11, 25, 30),
                                     'lastJobDuration': datetime.timedelta(days=1082, seconds=46920),
                                     'workedHours': [10, 5],
                                     'usedNames': ['Aida'],
                                     'courseScoresPerTerm': [[10, 8], [6, 7, 8]],
                                     '_label': 'person',
                                     '_id': {'offset': 0, 'table': 0}},
                                 2: {'ID': 2,
                                     'fName': 'Bob',
                                     'gender': 2,
                                     'isStudent': True,
                                     'isWorker': False,
                                     'age': 30,
                                     'eyeSight': 5.1,
                                     'birthdate': datetime.date(1900, 1, 1),
                                     'registerTime': datetime.datetime(2008, 11, 3, 15, 25, 30, 526),
                                     'lastJobDuration': datetime.timedelta(days=3750, seconds=46800, microseconds=24),
                                     'workedHours': [12, 8],
                                     'usedNames': ['Bobby'],
                                     'courseScoresPerTerm': [[8, 9], [9, 10]],
                                     '_label': 'person',
                                     '_id': {'offset': 1, 'table': 0}},
                                 3: {'ID': 3,
                                     'fName': 'Carol',
                                     'gender': 1,
                                     'isStudent': False,
                                     'isWorker': True,
                                     'age': 45,
                                     'eyeSight': 5.0,
                                     'birthdate': datetime.date(1940, 6, 22),
                                     'registerTime': datetime.datetime(1911, 8, 20, 2, 32, 21),
                                     'lastJobDuration': datetime.timedelta(days=2, seconds=1451),
                                     'workedHours': [4, 5],
                                     'usedNames': ['Carmen', 'Fred'],
                                     'courseScoresPerTerm': [[8, 10]],
                                     '_label': 'person',
                                     '_id': {'offset': 2, 'table': 0}},
                                 5: {'ID': 5,
                                     'fName': 'Dan',
                                     'gender': 2,
                                     'isStudent': False,
                                     'isWorker': True,
                                     'age': 20,
                                     'eyeSight': 4.8,
                                     'birthdate': datetime.date(1950, 7, 23),
                                     'registerTime': datetime.datetime(2031, 11, 30, 12, 25, 30),
                                     'lastJobDuration': datetime.timedelta(days=3750, seconds=46800, microseconds=24),
                                     'workedHours': [1, 9],
                                     'usedNames': ['Wolfeschlegelstein', 'Daniel'],
                                     'courseScoresPerTerm': [[7, 4], [8, 8], [9]],
                                     '_label': 'person',
                                     '_id': {'offset': 3, 'table': 0}},
                                 7: {'ID': 7,
                                     'fName': 'Elizabeth',
                                     'gender': 1,
                                     'isStudent': False,
                                     'isWorker': True,
                                     'age': 20,
                                     'eyeSight': 4.7,
                                     'birthdate': datetime.date(1980, 10, 26),
                                     'registerTime': datetime.datetime(1976, 12, 23, 11, 21, 42),
                                     'lastJobDuration': datetime.timedelta(days=2, seconds=1451),
                                     'workedHours': [2],
                                     'usedNames': ['Ein'],
                                     'courseScoresPerTerm': [[6], [7], [8]],
                                     '_label': 'person',
                                     '_id': {'offset': 4, 'table': 0}},
                                 8: {'ID': 8,
                                     'fName': 'Farooq',
                                     'gender': 2,
                                     'isStudent': True,
                                     'isWorker': False,
                                     'age': 25,
                                     'eyeSight': 4.5,
                                     'birthdate': datetime.date(1980, 10, 26),
                                     'registerTime': datetime.datetime(1972, 7, 31, 13, 22, 30, 678559),
                                     'lastJobDuration': datetime.timedelta(seconds=1080, microseconds=24000),
                                     'workedHours': [3, 4, 5, 6, 7],
                                     'usedNames': ['Fesdwe'],
                                     'courseScoresPerTerm': [[8]],
                                     '_label': 'person',
                                     '_id': {'offset': 5, 'table': 0}},
                                 9: {'ID': 9,
                                     'fName': 'Greg',
                                     'gender': 2,
                                     'isStudent': False,
                                     'isWorker': False,
                                     'age': 40,
                                     'eyeSight': 4.9,
                                     'birthdate': datetime.date(1980, 10, 26),
                                     'registerTime': datetime.datetime(1976, 12, 23, 4, 41, 42),
                                     'lastJobDuration': datetime.timedelta(days=3750, seconds=46800, microseconds=24),
                                     'workedHours': [1],
                                     'usedNames': ['Grad'],
                                     'courseScoresPerTerm': [[10]],
                                     '_label': 'person',
                                     '_id': {'offset': 6, 'table': 0}},
                                 10: {'ID': 10,
                                      'fName': 'Hubert Blaine Wolfeschlegelsteinhausenbergerdorff',
                                      'gender': 2,
                                      'isStudent': False,
                                      'isWorker': True,
                                      'age': 83,
                                      'eyeSight': 4.9,
                                      'birthdate': datetime.date(1990, 11, 27),
                                      'registerTime': datetime.datetime(2023, 2, 21, 13, 25, 30),
                                      'lastJobDuration': datetime.timedelta(days=1082, seconds=46920),
                                      'workedHours': [10, 11, 12, 3, 4, 5, 6, 7],
                                      'usedNames': ['Ad', 'De', 'Hi', 'Kye', 'Orlan'],
                                      'courseScoresPerTerm': [[7], [10], [6, 7]],
                                      '_label': 'person',
                                      '_id': {'offset': 7, 'table': 0}}}

TINY_SNB_ORGANISATIONS_GROUND_TRUTH = {1: {'ID': 1,
                                           'name': 'ABFsUni',
                                           'orgCode': 325,
                                           'mark': 3.7,
                                           'score': -2,
                                           'history': '10 years 5 months 13 hours 24 us',
                                           'licenseValidInterval': datetime.timedelta(days=1085),
                                           'rating': 1.0,
                                           '_label': 'organisation',
                                           '_id': {'offset': 0, 'table': 2}},
                                       4: {'ID': 4,
                                           'name': 'CsWork',
                                           'orgCode': 934,
                                           'mark': 4.1,
                                           'score': -100,
                                           'history': '2 years 4 days 10 hours',
                                           'licenseValidInterval': datetime.timedelta(days=9414),
                                           'rating': 0.78,
                                           '_label': 'organisation',
                                           '_id': {'offset': 1, 'table': 2}},
                                       6: {'ID': 6,
                                           'name': 'DEsWork',
                                           'orgCode': 824,
                                           'mark': 4.1,
                                           'score': 7,
                                           'history': '2 years 4 hours 22 us 34 minutes',
                                           'licenseValidInterval': datetime.timedelta(days=3, seconds=36000, microseconds=100000),
                                           'rating': 0.52,
                                           '_label': 'organisation',
                                           '_id': {'offset': 2, 'table': 2}}}

TINY_SNB_KNOWS_GROUND_TRUTH = {
    0: [2, 3, 5],
    2: [0, 3, 5],
    3: [0, 2, 5],
    5: [0, 2, 3],
    7: [8, 9],
}

TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH = {
    (0, 2): {'date': datetime.date(2021, 6, 30),
             'meetTime': datetime.datetime(1986, 10, 21, 21, 8, 31, 521000),
             'validInterval': datetime.timedelta(days=3750, seconds=46800, microseconds=24),
             'comments': ['rnme', 'm8sihsdnf2990nfiwf']},
    (0, 3): {'date': datetime.date(2021, 6, 30),
             'meetTime': datetime.datetime(1946, 8, 25, 19, 7, 22),
             'validInterval': datetime.timedelta(days=7232),
             'comments': ['njnojppo9u0jkmf', 'fjiojioh9h9h89hph']},
    (0, 5): {'date': datetime.date(2021, 6, 30),
             'meetTime': datetime.datetime(2012, 12, 11, 20, 7, 22),
             'validInterval': datetime.timedelta(days=10),
             'comments': ['ioji232', 'jifhe8w99u43434']},
    (2, 0): {'date': datetime.date(2021, 6, 30),
             'meetTime': datetime.datetime(1946, 8, 25, 19, 7, 22),
             'validInterval': datetime.timedelta(days=3750, seconds=46800, microseconds=24),
             'comments': ['2huh9y89fsfw23', '23nsihufhw723']},
    (2, 3): {'date': datetime.date(1950, 5, 14),
             'meetTime': datetime.datetime(1946, 8, 25, 19, 7, 22),
             'validInterval': datetime.timedelta(seconds=1380),
             'comments': ['fwehu9h9832wewew', '23u9h989sdfsss']},
    (2, 5): {'date': datetime.date(1950, 5, 14),
             'meetTime': datetime.datetime(2012, 12, 11, 20, 7, 22),
             'validInterval': datetime.timedelta(days=7232),
             'comments': ['fwh9y81232uisuiehuf', 'ewnuihxy8dyf232']},
    (3, 0): {'date': datetime.date(2021, 6, 30),
             'meetTime': datetime.datetime(2002, 7, 31, 11, 42, 53, 123420),
             'validInterval': datetime.timedelta(days=41, seconds=21600),
             'comments': ['fnioh8323aeweae34d', 'osd89e2ejshuih12']},
    (3, 2): {'date': datetime.date(1950, 5, 14),
             'meetTime': datetime.datetime(2007, 2, 12, 12, 11, 42, 123000),
             'validInterval': datetime.timedelta(seconds=1680, microseconds=30000),
             'comments': ['fwh983-sdjisdfji', 'ioh89y32r2huir']},
    (3, 5): {'date': datetime.date(2000, 1, 1),
             'meetTime': datetime.datetime(1998, 10, 2, 13, 9, 22, 423000),
             'validInterval': datetime.timedelta(microseconds=300000),
             'comments': ['psh989823oaaioe', 'nuiuah1nosndfisf']},
    (5, 0): {'date': datetime.date(2021, 6, 30),
             'meetTime': datetime.datetime(1936, 11, 2, 11, 2, 1),
             'validInterval': datetime.timedelta(microseconds=480),
             'comments': ['fwewe']},
    (5, 2): {'date': datetime.date(1950, 5, 14),
             'meetTime': datetime.datetime(1982, 11, 11, 13, 12, 5, 123000),
             'validInterval': datetime.timedelta(seconds=1380),
             'comments': ['fewh9182912e3',
                          'h9y8y89soidfsf',
                          'nuhudf78w78efw',
                          'hioshe0f9023sdsd']},
    (5, 3): {'date': datetime.date(2000, 1, 1),
             'meetTime': datetime.datetime(1999, 4, 21, 15, 12, 11, 420000),
             'validInterval': datetime.timedelta(days=2, microseconds=52000),
             'comments': ['23h9sdslnfowhu2932', 'shuhf98922323sf']},
    (7, 8): {'date': datetime.date(1905, 12, 12),
             'meetTime': datetime.datetime(2025, 1, 1, 11, 22, 33, 520000),
             'validInterval': datetime.timedelta(seconds=2878),
             'comments': ['ahu2333333333333', '12weeeeeeeeeeeeeeeeee']},
    (7, 9): {'date': datetime.date(1905, 12, 12),
             'meetTime': datetime.datetime(2020, 3, 1, 12, 11, 41, 655200),
             'validInterval': datetime.timedelta(seconds=2878),
             'comments': ['peweeeeeeeeeeeeeeeee', 'kowje9w0eweeeeeeeee']}
}


TINY_SNB_WORKS_AT_GROUND_TRUTH = {
    3: [4],
    5: [6],
    7: [6],
}

TINY_SNB_WORKS_AT_PROPERTIES_GROUND_TRUTH = {
    (3, 4): {'year': 2015},
    (5, 6): {'year': 2010},
    (7, 6): {'year': 2015}
}

TENSOR_LIST_GROUND_TRUTH = {
    0:	{
        'boolTensor': [True, False],
        'doubleTensor': [[0.1, 0.2], [0.3, 0.4]],
        'intTensor': [[[1, 2], [3, 4]], [[5, 6], [7, 8]]]
    },
    3: {
        'boolTensor': [True, False],
        'doubleTensor':	[[0.1, 0.2], [0.3, 0.4]],
        'intTensor': [[[3, 4], [5, 6]], [[7, 8], [9, 10]]]
    },
    4:	{
        'boolTensor': [False, True],
        'doubleTensor': [[0.4, 0.8], [0.7, 0.6]],
        'intTensor': [[[5, 6], [7, 8]], [[9, 10], [11, 12]]]
    },
    5: {
        'boolTensor': [True, True],
        'doubleTensor':	[[0.4, 0.9], [0.5, 0.2]],
        'intTensor': [[[7, 8], [9, 10]], [[11, 12], [13, 14]]]
    },
    6: {
        'boolTensor': [False, True],
        'doubleTensor': [[0.2, 0.4], [0.5, 0.1]],
        'intTensor': [[[9, 10], [11, 12]], [[13, 14], [15, 16]]]
    },
    8: {
        'boolTensor': [False, True],
        'doubleTensor': [[0.6, 0.4], [0.6, 0.1]],
        'intTensor': [[[11, 12], [13, 14]], [[15, 16], [17, 18]]]
    }
}

PERSONLONGSTRING_GROUND_TRUTH = {
    'AAAAAAAAAAAAAAAAAAAA': {
        'name': 'AAAAAAAAAAAAAAAAAAAA',
        'spouse': 'Bob',
    },
    'Bob': {
        'name': 'Bob',
        'spouse': 'AAAAAAAAAAAAAAAAAAAA',
    },
}

PERSONLONGSTRING_KNOWS_GROUND_TRUTH = {
    'AAAAAAAAAAAAAAAAAAAA': ['Bob'],
}


def test_to_torch_geometric_nodes_only(establish_connection):
    conn, _ = establish_connection
    query = "MATCH (p:person) return p"

    res = conn.execute(query)
    with warnings.catch_warnings(record=True) as ws:
        torch_geometric_data, pos_to_idx, unconverted_properties, _ = res.get_as_torch_geometric()
    warnings_ground_truth = set([
        "Property person.courseScoresPerTerm cannot be converted to Tensor (likely due to nested list of variable length). The property is marked as unconverted.",
        "Property person.lastJobDuration of type INTERVAL is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.registerTime of type TIMESTAMP is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.birthdate of type DATE is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.fName of type STRING is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.workedHours has an inconsistent shape. The property is marked as unconverted.",
        "Property person.usedNames of type STRING is not supported by torch_geometric. The property is marked as unconverted.",
    ])
    assert len(ws) == 7
    for w in ws:
        assert str(w.message) in warnings_ground_truth

    assert torch_geometric_data.ID.shape == torch.Size([8])
    assert torch_geometric_data.ID.dtype == torch.int64
    for i in range(8):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['ID'] == torch_geometric_data.ID[i].item()

    assert torch_geometric_data.gender.shape == torch.Size([8])
    assert torch_geometric_data.gender.dtype == torch.int64
    for i in range(8):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['gender'] == torch_geometric_data.gender[i].item()

    assert torch_geometric_data.isStudent.shape == torch.Size([8])
    assert torch_geometric_data.isStudent.dtype == torch.bool
    for i in range(8):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['isStudent'] == torch_geometric_data.isStudent[i].item()

    assert torch_geometric_data.isWorker.shape == torch.Size([8])
    assert torch_geometric_data.isWorker.dtype == torch.bool
    for i in range(8):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['isWorker'] == torch_geometric_data.isWorker[i].item()

    assert torch_geometric_data.age.shape == torch.Size([8])
    assert torch_geometric_data.age.dtype == torch.int64
    for i in range(8):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['age'] == torch_geometric_data.age[i].item()

    assert torch_geometric_data.eyeSight.shape == torch.Size([8])
    assert torch_geometric_data.eyeSight.dtype == torch.float32
    for i in range(8):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]['eyeSight'] - \
            torch_geometric_data.eyeSight[i].item() < 1e-6

    assert len(unconverted_properties) == 7
    assert 'courseScoresPerTerm' in unconverted_properties
    for i in range(8):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['courseScoresPerTerm'] == unconverted_properties['courseScoresPerTerm'][i]
    assert 'lastJobDuration' in unconverted_properties
    for i in range(8):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['lastJobDuration'] == unconverted_properties['lastJobDuration'][i]
    assert 'registerTime' in unconverted_properties
    for i in range(8):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['registerTime'] == unconverted_properties['registerTime'][i]
    assert 'birthdate' in unconverted_properties
    for i in range(8):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['birthdate'] == unconverted_properties['birthdate'][i]
    assert 'fName' in unconverted_properties
    for i in range(8):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['fName'] == unconverted_properties['fName'][i]
    assert 'usedNames' in unconverted_properties
    for i in range(8):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['usedNames'] == unconverted_properties['usedNames'][i]

    assert 'workedHours' in unconverted_properties
    for i in range(8):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['workedHours'] == unconverted_properties['workedHours'][i]


def test_to_torch_geometric_homogeneous_graph(establish_connection):
    conn, _ = establish_connection
    query = "MATCH (p:person)-[r:knows]->(q:person) RETURN p, r, q"

    res = conn.execute(query)
    with warnings.catch_warnings(record=True) as ws:
        torch_geometric_data, pos_to_idx, unconverted_properties, edge_properties = res.get_as_torch_geometric()
    warnings_ground_truth = set([
        "Property person.courseScoresPerTerm cannot be converted to Tensor (likely due to nested list of variable length). The property is marked as unconverted.",
        "Property person.lastJobDuration of type INTERVAL is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.registerTime of type TIMESTAMP is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.birthdate of type DATE is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.fName of type STRING is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.workedHours has an inconsistent shape. The property is marked as unconverted.",
        "Property person.usedNames of type STRING is not supported by torch_geometric. The property is marked as unconverted.",
    ])
    assert len(ws) == 7
    for w in ws:
        assert str(w.message) in warnings_ground_truth

    assert torch_geometric_data.ID.shape == torch.Size([7])
    assert torch_geometric_data.ID.dtype == torch.int64
    for i in range(7):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['ID'] == torch_geometric_data.ID[i].item()

    assert torch_geometric_data.gender.shape == torch.Size([7])
    assert torch_geometric_data.gender.dtype == torch.int64
    for i in range(7):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['gender'] == torch_geometric_data.gender[i].item()

    assert torch_geometric_data.isStudent.shape == torch.Size([7])
    assert torch_geometric_data.isStudent.dtype == torch.bool
    for i in range(7):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['isStudent'] == torch_geometric_data.isStudent[i].item()

    assert torch_geometric_data.isWorker.shape == torch.Size([7])
    assert torch_geometric_data.isWorker.dtype == torch.bool
    for i in range(7):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['isWorker'] == torch_geometric_data.isWorker[i].item()

    assert torch_geometric_data.age.shape == torch.Size([7])
    assert torch_geometric_data.age.dtype == torch.int64
    for i in range(7):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['age'] == torch_geometric_data.age[i].item()

    assert torch_geometric_data.eyeSight.shape == torch.Size([7])
    assert torch_geometric_data.eyeSight.dtype == torch.float32
    for i in range(7):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]]['eyeSight'] - \
            torch_geometric_data.eyeSight[i].item() < 1e-6

    assert len(unconverted_properties) == 7
    assert 'courseScoresPerTerm' in unconverted_properties
    for i in range(7):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['courseScoresPerTerm'] == unconverted_properties['courseScoresPerTerm'][i]
    assert 'lastJobDuration' in unconverted_properties
    for i in range(7):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['lastJobDuration'] == unconverted_properties['lastJobDuration'][i]
    assert 'registerTime' in unconverted_properties
    for i in range(7):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['registerTime'] == unconverted_properties['registerTime'][i]
    assert 'birthdate' in unconverted_properties
    for i in range(7):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['birthdate'] == unconverted_properties['birthdate'][i]
    assert 'fName' in unconverted_properties
    for i in range(7):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['fName'] == unconverted_properties['fName'][i]
    assert 'usedNames' in unconverted_properties
    for i in range(7):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['usedNames'] == unconverted_properties['usedNames'][i]

    assert 'workedHours' in unconverted_properties
    for i in range(7):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx[i]
                                             ]['workedHours'] == unconverted_properties['workedHours'][i]

    assert torch_geometric_data.edge_index.shape == torch.Size([2, 14])
    for i in range(14):
        src, dst = torch_geometric_data.edge_index[0][i].item(
        ), torch_geometric_data.edge_index[1][i].item()
        assert src in pos_to_idx
        assert dst in pos_to_idx
        assert src != dst
        assert pos_to_idx[dst] in TINY_SNB_KNOWS_GROUND_TRUTH[pos_to_idx[src]]

    assert len(edge_properties) == 4
    assert 'date' in edge_properties
    assert 'meetTime' in edge_properties
    assert 'validInterval' in edge_properties
    assert 'comments' in edge_properties

    for i in range(14):
        src, dst = torch_geometric_data.edge_index[0][i].item(
        ), torch_geometric_data.edge_index[1][i].item()
        orginal_src = pos_to_idx[src]
        orginal_dst = pos_to_idx[dst]
        assert (orginal_src, orginal_dst) in TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH
        assert TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH[(
            orginal_src, orginal_dst)]['date'] == edge_properties['date'][i]
        assert TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH[(
            orginal_src, orginal_dst)]['meetTime'] == edge_properties['meetTime'][i]
        assert TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH[(
            orginal_src, orginal_dst)]['validInterval'] == edge_properties['validInterval'][i]
        assert TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH[(
            orginal_src, orginal_dst)]['comments'] == edge_properties['comments'][i]


def test_to_torch_geometric_heterogeneous_graph(establish_connection):
    conn, _ = establish_connection
    query = "MATCH (p:person)-[r1:knows]->(q:person)-[r2:workAt]->(o:organisation) RETURN p, q, o, r1, r2"

    res = conn.execute(query)
    with warnings.catch_warnings(record=True) as ws:
        torch_geometric_data, pos_to_idx, unconverted_properties, edge_properties = res.get_as_torch_geometric()

    assert len(ws) == 9
    warnings_ground_truth = set([
        "Property organisation.name of type STRING is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.courseScoresPerTerm cannot be converted to Tensor (likely due to nested list of variable length). The property is marked as unconverted.",
        "Property person.lastJobDuration of type INTERVAL is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.registerTime of type TIMESTAMP is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.birthdate of type DATE is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.fName of type STRING is not supported by torch_geometric. The property is marked as unconverted.",
        "Property organisation.history of type STRING is not supported by torch_geometric. The property is marked as unconverted.",
        "Property person.usedNames of type STRING is not supported by torch_geometric. The property is marked as unconverted.",
        "Property organisation.licenseValidInterval of type INTERVAL is not supported by torch_geometric. The property is marked as unconverted.",
    ])

    for w in ws:
        assert str(w.message) in warnings_ground_truth

    assert torch_geometric_data['person'].ID.shape == torch.Size([4])
    assert torch_geometric_data['person'].ID.dtype == torch.int64
    for i in range(4):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx['person'][i]
                                             ]['ID'] == torch_geometric_data['person'].ID[i].item()

    assert torch_geometric_data['person'].gender.shape == torch.Size([4])
    assert torch_geometric_data['person'].gender.dtype == torch.int64
    for i in range(4):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx['person'][i]
                                             ]['gender'] == torch_geometric_data['person'].gender[i].item()

    assert torch_geometric_data['person'].isStudent.shape == torch.Size([4])
    assert torch_geometric_data['person'].isStudent.dtype == torch.bool
    for i in range(4):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx['person'][i]
                                             ]['isStudent'] == torch_geometric_data['person'].isStudent[i].item()

    assert torch_geometric_data['person'].isWorker.shape == torch.Size([4])
    assert torch_geometric_data['person'].isWorker.dtype == torch.bool
    for i in range(4):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx['person'][i]
                                             ]['isWorker'] == torch_geometric_data['person'].isWorker[i].item()

    assert torch_geometric_data['person'].age.shape == torch.Size([4])
    assert torch_geometric_data['person'].age.dtype == torch.int64
    for i in range(4):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx['person'][i]
                                             ]['age'] == torch_geometric_data['person'].age[i].item()

    assert torch_geometric_data['person'].eyeSight.shape == torch.Size([4])
    assert torch_geometric_data['person'].eyeSight.dtype == torch.float32
    for i in range(4):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx['person'][i]]['eyeSight'] - \
            torch_geometric_data['person'].eyeSight[i].item() < 1e-6

    assert 'person' in unconverted_properties
    assert len(unconverted_properties['person']) == 6
    assert 'courseScoresPerTerm' in unconverted_properties['person']
    for i in range(4):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx['person'][i]
                                             ]['courseScoresPerTerm'] == unconverted_properties['person']['courseScoresPerTerm'][i]
    assert 'lastJobDuration' in unconverted_properties['person']
    for i in range(4):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx['person'][i]
                                             ]['lastJobDuration'] == unconverted_properties['person']['lastJobDuration'][i]
    assert 'registerTime' in unconverted_properties['person']
    for i in range(4):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx['person'][i]
                                             ]['registerTime'] == unconverted_properties['person']['registerTime'][i]
    assert 'birthdate' in unconverted_properties['person']
    for i in range(4):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx['person'][i]
                                             ]['birthdate'] == unconverted_properties['person']['birthdate'][i]
    assert 'fName' in unconverted_properties['person']
    for i in range(4):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx['person'][i]
                                             ]['fName'] == unconverted_properties['person']['fName'][i]
    assert 'usedNames' in unconverted_properties['person']
    for i in range(4):
        assert TINY_SNB_PERSONS_GROUND_TRUTH[pos_to_idx['person'][i]
                                             ]['usedNames'] == unconverted_properties['person']['usedNames'][i]

    assert torch_geometric_data['person', 'person'].edge_index.shape == torch.Size([
                                                                                   2, 6])
    for i in range(3):
        src, dst = torch_geometric_data['person', 'person'].edge_index[0][i].item(
        ), torch_geometric_data['person', 'person'].edge_index[1][i].item()
        assert src in pos_to_idx['person']
        assert dst in pos_to_idx['person']
        assert src != dst
        assert pos_to_idx['person'][dst] in TINY_SNB_KNOWS_GROUND_TRUTH[pos_to_idx['person'][src]]

    assert len(edge_properties['person', 'person']) == 4
    assert 'date' in edge_properties['person', 'person']
    assert 'meetTime' in edge_properties['person', 'person']
    assert 'validInterval' in edge_properties['person', 'person']
    assert 'comments' in edge_properties['person', 'person']
    for i in range(3):
        src, dst = torch_geometric_data['person', 'person'].edge_index[0][i].item(
        ), torch_geometric_data['person', 'person'].edge_index[1][i].item()
        original_src, original_dst = pos_to_idx['person'][src], pos_to_idx['person'][dst]
        assert (original_src, original_dst) in TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH
        assert TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH[(
            original_src, original_dst)]['date'] == edge_properties['person', 'person']['date'][i]
        assert TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH[(
            original_src, original_dst)]['meetTime'] == edge_properties['person', 'person']['meetTime'][i]
        assert TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH[(
            original_src, original_dst)]['validInterval'] == edge_properties['person', 'person']['validInterval'][i]
        assert TINY_SNB_KNOWS_PROPERTIES_GROUND_TRUTH[(
            original_src, original_dst)]['comments'] == edge_properties['person', 'person']['comments'][i]

    assert torch_geometric_data['organisation'].ID.shape == torch.Size([2])
    assert torch_geometric_data['organisation'].ID.dtype == torch.int64
    for i in range(2):
        assert TINY_SNB_ORGANISATIONS_GROUND_TRUTH[pos_to_idx['organisation'][i]
                                                   ]['ID'] == torch_geometric_data['organisation'].ID[i].item()

    assert torch_geometric_data['organisation'].orgCode.shape == torch.Size([
        2])
    assert torch_geometric_data['organisation'].orgCode.dtype == torch.int64
    for i in range(2):
        assert TINY_SNB_ORGANISATIONS_GROUND_TRUTH[pos_to_idx['organisation'][i]
                                                   ]['orgCode'] == torch_geometric_data['organisation'].orgCode[i].item()

    assert torch_geometric_data['organisation'].mark.shape == torch.Size([2])
    assert torch_geometric_data['organisation'].mark.dtype == torch.float32
    for i in range(2):
        assert TINY_SNB_ORGANISATIONS_GROUND_TRUTH[pos_to_idx['organisation'][i]
                                                   ]['mark'] - torch_geometric_data['organisation'].mark[i].item() < 1e-6

    assert torch_geometric_data['organisation'].score.shape == torch.Size([2])
    assert torch_geometric_data['organisation'].score.dtype == torch.int64
    for i in range(2):
        assert TINY_SNB_ORGANISATIONS_GROUND_TRUTH[pos_to_idx['organisation'][i]
                                                   ]['score'] - torch_geometric_data['organisation'].score[i].item() < 1e-6

    assert torch_geometric_data['organisation'].rating.shape == torch.Size([2])
    assert torch_geometric_data['organisation'].rating.dtype == torch.float32
    for i in range(2):
        assert TINY_SNB_ORGANISATIONS_GROUND_TRUTH[pos_to_idx['organisation'][i]
                                                   ]['rating'] - torch_geometric_data['organisation'].rating[i].item() < 1e-6

    assert 'organisation' in unconverted_properties
    assert len(unconverted_properties['organisation']) == 3
    assert 'name' in unconverted_properties['organisation']
    for i in range(2):
        assert TINY_SNB_ORGANISATIONS_GROUND_TRUTH[pos_to_idx['organisation'][i]
                                                   ]['name'] == unconverted_properties['organisation']['name'][i]

    assert 'history' in unconverted_properties['organisation']
    for i in range(2):
        assert TINY_SNB_ORGANISATIONS_GROUND_TRUTH[pos_to_idx['organisation'][i]
                                                   ]['history'] == unconverted_properties['organisation']['history'][i]

    assert 'licenseValidInterval' in unconverted_properties['organisation']
    for i in range(2):
        assert TINY_SNB_ORGANISATIONS_GROUND_TRUTH[pos_to_idx['organisation'][i]
                                                   ]['licenseValidInterval'] == unconverted_properties['organisation']['licenseValidInterval'][i]

    assert torch_geometric_data['person', 'organisation'].edge_index.shape == torch.Size([
        2, 2])
    for i in range(2):
        src, dst = torch_geometric_data['person', 'organisation'].edge_index[0][i].item(
        ), torch_geometric_data['person', 'organisation'].edge_index[1][i].item()
        assert src in pos_to_idx['person']
        assert dst in pos_to_idx['organisation']
        assert src != dst
        assert pos_to_idx['organisation'][dst] in TINY_SNB_WORKS_AT_GROUND_TRUTH[pos_to_idx['person'][src]]
    assert len(edge_properties['person', 'organisation']) == 1
    assert 'year' in edge_properties['person', 'organisation']
    for i in range(2):
        src, dst = torch_geometric_data['person', 'organisation'].edge_index[0][i].item(
        ), torch_geometric_data['person', 'organisation'].edge_index[1][i].item()
        original_src, original_dst = pos_to_idx['person'][src], pos_to_idx['organisation'][dst]
        assert TINY_SNB_WORKS_AT_PROPERTIES_GROUND_TRUTH[(
            original_src, original_dst)]['year'] == edge_properties['person', 'organisation']['year'][i]


def test_to_torch_geometric_multi_dimensonal_lists(establish_connection):
    conn, _ = establish_connection
    query = "MATCH (t:tensor) RETURN t"

    res = conn.execute(query)
    with warnings.catch_warnings(record=True) as ws:
        torch_geometric_data, pos_to_idx, unconverted_properties, _ = res.get_as_torch_geometric()
    assert len(ws) == 1
    assert str(ws[0].message) == "Property tensor.oneDimInt has a null value. torch_geometric does not support null values. The property is marked as unconverted."

    bool_list = []
    float_list = []
    int_list = []

    for i in range(len(pos_to_idx)):
        idx = pos_to_idx[i]
        bool_list.append(TENSOR_LIST_GROUND_TRUTH[idx]['boolTensor'])
        float_list.append(TENSOR_LIST_GROUND_TRUTH[idx]['doubleTensor'])
        int_list.append(TENSOR_LIST_GROUND_TRUTH[idx]['intTensor'])

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


def test_to_torch_geometric_no_properties_converted(establish_connection):
    conn, _ = establish_connection
    query = "MATCH (p:personLongString)-[r:knowsLongString]->(q:personLongString) RETURN p, r, q"

    res = conn.execute(query)
    with warnings.catch_warnings(record=True) as ws:
        torch_geometric_data, pos_to_idx, unconverted_properties, _ = res.get_as_torch_geometric()
    assert len(ws) == 3
    warnings_ground_truth = set([
        "Property personLongString.name of type STRING is not supported by torch_geometric. The property is marked as unconverted.",
        "Property personLongString.spouse of type STRING is not supported by torch_geometric. The property is marked as unconverted.",
        "No nodes found or all node properties are not converted."])
    for w in ws:
        assert str(w.message) in warnings_ground_truth
    assert torch_geometric_data['personLongString'] == {}
    assert torch_geometric_data['personLongString',
                                'personLongString'].edge_index.shape == torch.Size([2, 1])

    for i in range(1):
        src, dst = torch_geometric_data['personLongString', 'personLongString'].edge_index[0][i].item(
        ), torch_geometric_data['personLongString', 'personLongString'].edge_index[1][i].item()
        assert src in pos_to_idx['personLongString']
        assert dst in pos_to_idx['personLongString']
        assert src != dst
        assert pos_to_idx['personLongString'][dst] in PERSONLONGSTRING_KNOWS_GROUND_TRUTH[pos_to_idx['personLongString'][src]]

    assert len(unconverted_properties) == 1
    assert len(unconverted_properties['personLongString']) == 2

    assert 'spouse' in unconverted_properties['personLongString']
    assert len(unconverted_properties['personLongString']['spouse']) == 2
    for i in range(2):
        assert PERSONLONGSTRING_GROUND_TRUTH[pos_to_idx['personLongString'][i]
                                             ]['spouse'] == unconverted_properties['personLongString']['spouse'][i]

    assert 'name' in unconverted_properties['personLongString']
    for i in range(2):
        assert PERSONLONGSTRING_GROUND_TRUTH[pos_to_idx['personLongString'][i]
                                             ]['name'] == unconverted_properties['personLongString']['name'][i]
