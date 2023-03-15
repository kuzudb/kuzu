#include "include/py_database.h"

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
        systemConfig.defaultPageBufferPoolSize =
            bufferPoolSize * StorageConstants::DEFAULT_PAGES_BUFFER_RATIO;
        systemConfig.largePageBufferPoolSize =
            bufferPoolSize * StorageConstants::LARGE_PAGES_BUFFER_RATIO;
    }
    database = std::make_unique<Database>(databasePath, systemConfig);
}
