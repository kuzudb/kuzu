#include "include/py_database.h"

#include <memory>

using namespace kuzu::common;

void PyDatabase::initialize(py::handle& m) {
    py::class_<PyDatabase>(m, "Database")
        .def(py::init<const std::string&, uint64_t>(), py::arg("database_path"),
            py::arg("buffer_pool_size") = 0)
        .def("set_logging_level", &PyDatabase::setLoggingLevel, py::arg("logging_level"));
}

PyDatabase::PyDatabase(const std::string& databasePath, uint64_t bufferPoolSize) {
    auto systemConfig = SystemConfig();
    if (bufferPoolSize > 0) {
        systemConfig.bufferPoolSize = bufferPoolSize;
    }
    database = std::make_unique<Database>(databasePath, systemConfig);
}
