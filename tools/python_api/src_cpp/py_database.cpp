#include "include/py_database.h"

#include <memory>

#include "include/cached_import/py_cached_import.h"
#include "main/version.h"
#include "pandas/pandas_scan.h"
#include "pyarrow/pyarrow_scan.h"

using namespace kuzu::common;

void PyDatabase::initialize(py::handle& m) {
    py::class_<PyDatabase>(m, "Database")
        .def(py::init<const std::string&, uint64_t, uint64_t, bool, bool, uint64_t>(),
            py::arg("database_path"), py::arg("buffer_pool_size") = 0,
            py::arg("max_num_threads") = 0, py::arg("compression") = true,
            py::arg("read_only") = false, py::arg("max_db_size") = (uint64_t)1 << 43)
        .def("scan_node_table_as_int64", &PyDatabase::scanNodeTable<std::int64_t>,
            py::arg("table_name"), py::arg("prop_name"), py::arg("indices"), py::arg("np_array"),
            py::arg("num_threads"))
        .def("scan_node_table_as_int32", &PyDatabase::scanNodeTable<std::int32_t>,
            py::arg("table_name"), py::arg("prop_name"), py::arg("indices"), py::arg("np_array"),
            py::arg("num_threads"))
        .def("scan_node_table_as_int16", &PyDatabase::scanNodeTable<std::int16_t>,
            py::arg("table_name"), py::arg("prop_name"), py::arg("indices"), py::arg("np_array"),
            py::arg("num_threads"))
        .def("scan_node_table_as_double", &PyDatabase::scanNodeTable<double>, py::arg("table_name"),
            py::arg("prop_name"), py::arg("indices"), py::arg("np_array"), py::arg("num_threads"))
        .def("scan_node_table_as_float", &PyDatabase::scanNodeTable<float>, py::arg("table_name"),
            py::arg("prop_name"), py::arg("indices"), py::arg("np_array"), py::arg("num_threads"))
        .def("scan_node_table_as_bool", &PyDatabase::scanNodeTable<bool>, py::arg("table_name"),
            py::arg("prop_name"), py::arg("indices"), py::arg("np_array"), py::arg("num_threads"))
        .def("close", &PyDatabase::close)
        .def_static("get_version", &PyDatabase::getVersion)
        .def_static("get_storage_version", &PyDatabase::getStorageVersion);
}

py::str PyDatabase::getVersion() {
    return py::str(Version::getVersion());
}

uint64_t PyDatabase::getStorageVersion() {
    return Version::getStorageVersion();
}

PyDatabase::PyDatabase(const std::string& databasePath, uint64_t bufferPoolSize,
    uint64_t maxNumThreads, bool compression, bool readOnly, uint64_t maxDBSize) {
    auto systemConfig =
        SystemConfig(bufferPoolSize, maxNumThreads, compression, readOnly, maxDBSize);
    database = std::make_unique<Database>(databasePath, systemConfig);
    database->addTableFunction(kuzu::PandasScanFunction::name,
        kuzu::PandasScanFunction::getFunctionSet());
    storageDriver = std::make_unique<kuzu::main::StorageDriver>(database.get());
    py::gil_scoped_acquire acquire;
    if (kuzu::importCache.get() == nullptr) {
        kuzu::importCache = std::make_shared<kuzu::PythonCachedImport>();
    }
}

PyDatabase::~PyDatabase() {}

void PyDatabase::close() {
    database.reset();
}

template<class T>
void PyDatabase::scanNodeTable(const std::string& tableName, const std::string& propName,
    const py::array_t<uint64_t>& indices, py::array_t<T>& result, int numThreads) {
    auto indices_buffer_info = indices.request(false);
    auto indices_buffer = static_cast<uint64_t*>(indices_buffer_info.ptr);
    auto nodeOffsets = (offset_t*)indices_buffer;
    auto result_buffer_info = result.request();
    auto result_buffer = (uint8_t*)result_buffer_info.ptr;
    auto size = indices.size();
    storageDriver->scan(tableName, propName, nodeOffsets, size, result_buffer, numThreads);
}
