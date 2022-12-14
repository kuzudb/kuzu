#include "include/py_database.h"

PyDatabase::PyDatabase(const string& databasePath, uint64_t bufferPoolSize) {
    auto systemConfig = SystemConfig();
    if (bufferPoolSize > 0) {
        systemConfig.defaultPageBufferPoolSize =
            bufferPoolSize * StorageConfig::DEFAULT_PAGES_BUFFER_RATIO;
        systemConfig.largePageBufferPoolSize =
            bufferPoolSize * StorageConfig::LARGE_PAGES_BUFFER_RATIO;
    }
    database = make_unique<Database>(DatabaseConfig(databasePath), systemConfig);
    auto atexit = py::module_::import("atexit");
    atexit.attr("register")(py::cpp_function([&]() {
        if (database) {
            database.reset();
        }
    }));
}

void PyDatabase::initialize(py::handle& m) {
    py::class_<PyDatabase>(m, "database")
        .def(py::init<const string&, uint64_t>(), py::arg("database_path"),
            py::arg("buffer_pool_size") = 0)
        .def("resize_buffer_manager", &PyDatabase::resizeBufferManager, py::arg("new_size"))
        .def("set_logging_level", &PyDatabase::setLoggingLevel, py::arg("logging_level"));
}

void PyDatabase::resizeBufferManager(uint64_t newSize) {
    database->resizeBufferManager(newSize);
}
