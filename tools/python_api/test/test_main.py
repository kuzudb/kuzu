import pytest

from test_arrow import *
from test_datatype import *
from test_df import *
from test_exception import *
from test_get_header import *
from test_networkx import *
from test_parameter import *
from test_prepared_statement import *
from test_query_result import *
from test_query_result_close import *
from test_timeout import *
from test_torch_geometric import *
from test_torch_geometric_remote_backend import *
from test_write_to_csv import *
from test_scan_pandas import *

if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))
