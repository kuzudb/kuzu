#include "include/py_database.h"

#include "main/storage_driver.h"

using namespace kuzu::common;

void PyDatabase::initialize(py::handle& m) {
    py::class_<PyDatabase>(m, "Database")
        .def(py::init<const std::string&, uint64_t>(), py::arg("database_path"),
            py::arg("buffer_pool_size") = 0)
        .def("resize_buffer_manager", &PyDatabase::resizeBufferManager, py::arg("new_size"))
        .def("set_logging_level", &PyDatabase::setLoggingLevel, py::arg("logging_level"))
        .def("scan_node_table_as_int64", &PyDatabase::scanNodeTable<std::int64_t>,
            py::return_value_policy::take_ownership, py::arg("table_name"), py::arg("prop_name"),
            py::arg("indices"))
        .def("scan_node_table_as_double", &PyDatabase::scanNodeTable<std::double_t>,
            py::return_value_policy::take_ownership, py::arg("table_name"), py::arg("prop_name"),
            py::arg("indices"))
        .def("scan_node_table_as_bool", &PyDatabase::scanNodeTable<bool>,
            py::return_value_policy::take_ownership, py::arg("table_name"), py::arg("prop_name"),
            py::arg("indices"));
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

void PyDatabase::resizeBufferManager(uint64_t newSize) {
    database->resizeBufferManager(newSize);
}

template<class T>
py::array_t<T> PyDatabase::scanNodeTable(
    const std::string& tableName, const std::string& propName, py::array_t<uint64_t> indices) {
    auto buf = indices.request(false);
    auto ptr1 = static_cast<uint64_t*>(buf.ptr);
    auto size = indices.size();
    auto nodeOffsets = (offset_t*)ptr1;
    auto storageDriver = std::make_unique<StorageDriver>(database.get());
    auto scanResult = storageDriver->scan(tableName, propName, nodeOffsets, size);
    auto buffer = (T*)(scanResult.first).get();
    auto bufferSize = scanResult.second;
    auto numberOfItems = bufferSize / sizeof(T);
    return py::array_t<T>(py::buffer_info(buffer, sizeof(T), py::format_descriptor<T>::format(), 1,
        std::vector<size_t>{numberOfItems}, std::vector<size_t>{sizeof(T)}));
}