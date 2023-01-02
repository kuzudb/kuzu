#include "include/py_connection.h"
#include "include/py_database.h"

void bindEnumTypes(py::module& m) {
    py::enum_<spdlog::level::level_enum>(m, "loggingLevel")
        .value("debug", spdlog::level::level_enum::debug)
        .value("info", spdlog::level::level_enum::info)
        .value("err", spdlog::level::level_enum::err)
        .export_values();
}

void bind(py::module& m) {
    bindEnumTypes(m);
    PyDatabase::initialize(m);
    PyConnection::initialize(m);
    PyQueryResult::initialize(m);

    m.doc() = "Kuzu is an embedded graph database";
}

PYBIND11_MODULE(_kuzu, m) {
    bind(m);
}
