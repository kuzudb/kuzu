#include "include/py_connection.h"
#include "include/py_database.h"
#include "include/py_query_result_converter.h"

void bind(py::module& m) {
    PyDatabase::initialize(m);
    PyConnection::initialize(m);
    PyQueryResult::initialize(m);

    m.doc() = "GrpahflowDB is an embedded graph database";
}

PYBIND11_MODULE(_kuzu, m) {
    bind(m);
}
