#include "include/py_database.h"

void PyDatabase::initialize(py::handle& m) {
    py::class_<PyDatabase>(m, "database")
        .def(py::init<const string&, uint64_t, uint64_t>(), py::arg("database_path"),
            py::arg("default_page_buffer_pool_size") = 0,
            py::arg("large_page_buffer_pool_size") = 0)
        .def("resize_buffer_manager", &PyDatabase::resizeBufferManager, py::arg("new_size"));
}

PyDatabase::PyDatabase(const string& databasePath, uint64_t defaultPageBufferPoolSize,
    uint64_t largePageBufferPoolSize) {
    auto systemConfig = SystemConfig();
    if (defaultPageBufferPoolSize > 0) {
        systemConfig.defaultPageBufferPoolSize = defaultPageBufferPoolSize;
    }
    if (largePageBufferPoolSize > 0) {
        systemConfig.largePageBufferPoolSize = largePageBufferPoolSize;
    }
    database = make_unique<Database>(DatabaseConfig(databasePath), systemConfig);
}

void PyDatabase::resizeBufferManager(uint64_t newSize) {
    database->resizeBufferManager(newSize);
}
