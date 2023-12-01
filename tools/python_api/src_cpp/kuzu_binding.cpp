#include "include/py_connection.h"
#include "include/py_database.h"
#include "include/py_prepared_statement.h"
#include "include/py_query_result.h"

void bind(py::module& m) {
    PyDatabase::initialize(m);
    PyConnection::initialize(m);
    PyPreparedStatement::initialize(m);
    PyQueryResult::initialize(m);
}

PYBIND11_MODULE(_kuzu, m) {
    bind(m);
}
