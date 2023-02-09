#include "include/py_connection.h"
#include "include/py_database.h"
#include "spdlog/spdlog.h"

void bind(py::module& m) {
    PyDatabase::initialize(m);
    PyConnection::initialize(m);
    PyQueryResult::initialize(m);

    m.doc() = "Kuzu is an embedded graph database";
}

PYBIND11_MODULE(_kuzu, m) {
    bind(m);
}
