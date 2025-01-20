#pragma once

#include "cached_import/py_cached_import.h"
#include "main/kuzu.h"
#include "main/storage_driver.h"
#include "pybind_include.h" // IWYU pragma: keep (used for py:: namespace)
#define PYBIND11_DETAILED_ERROR_MESSAGES
using namespace kuzu::main;

class PyDatabase {
    friend class PyConnection;

public:
    static void initialize(py::handle& m);

    static py::str getVersion();

    static uint64_t getStorageVersion();

    explicit PyDatabase(const std::string& databasePath, uint64_t bufferPoolSize,
        uint64_t maxNumThreads, bool compression, bool readOnly, uint64_t maxDBSize,
        bool autoCheckpoint, int64_t checkpointThreshold);

    ~PyDatabase();

    void close();

    template<class T>
    void scanNodeTable(const std::string& tableName, const std::string& propName,
        const py::array_t<uint64_t>& indices, py::array_t<T>& result, int numThreads);

private:
    std::unique_ptr<Database> database;
    std::unique_ptr<StorageDriver> storageDriver;
};
