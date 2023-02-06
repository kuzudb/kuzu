#include "include/py_database.h"

using namespace kuzu::common;

void PyDatabase::initialize(py::handle& m) {
    py::class_<PyDatabase>(m, "Database")
        .def(py::init<const std::string&, uint64_t>(), py::arg("database_path"),
            py::arg("buffer_pool_size") = 0)
        .def("resize_buffer_manager", &PyDatabase::resizeBufferManager, py::arg("new_size"))
        .def("set_logging_level", &PyDatabase::setLoggingLevel, py::arg("logging_level"));
}

PyDatabase::PyDatabase(const std::string& databasePath, uint64_t bufferPoolSize) {
    auto systemConfig = SystemConfig();
    if (bufferPoolSize > 0) {
        systemConfig.defaultPageBufferPoolSize =
            bufferPoolSize * StorageConfig::DEFAULT_PAGES_BUFFER_RATIO;
        systemConfig.largePageBufferPoolSize =
            bufferPoolSize * StorageConfig::LARGE_PAGES_BUFFER_RATIO;
    }
    database = std::make_unique<Database>(DatabaseConfig(databasePath), systemConfig);
}

void PyDatabase::resizeBufferManager(uint64_t newSize) {
    database->resizeBufferManager(newSize);
}
