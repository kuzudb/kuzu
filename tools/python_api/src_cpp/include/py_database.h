#pragma once

#include "main/kuzu.h"
#include "main/storage_driver.h"
#include "pybind_include.h" // IWYU pragma: keep (used for py:: namespace)
#define PYBIND11_DETAILED_ERROR_MESSAGES
using namespace kuzu::main;

class PyDatabase {
    friend class PyConnection;

public:
    inline void setLoggingLevel(std::string logging_level) {
        database->setLoggingLevel(std::move(logging_level));
    }

    static void initialize(py::handle& m);

    explicit PyDatabase(const std::string& databasePath, uint64_t bufferPoolSize,
        uint64_t maxNumThreads, bool compression, bool readOnly);

    ~PyDatabase() = default;

    template<class T>
    void scanNodeTable(const std::string& tableName, const std::string& propName,
        const py::array_t<uint64_t>& indices, py::array_t<T>& result, int numThreads);

private:
    std::unique_ptr<Database> database;
    std::unique_ptr<StorageDriver> storageDriver;
};
