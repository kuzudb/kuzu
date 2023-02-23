#pragma once

#include "main/kuzu.h"
#include "pybind_include.h"

using namespace kuzu::main;

class PyDatabase {
    friend class PyConnection;

public:
    inline void setLoggingLevel(std::string logging_level) {
        database->setLoggingLevel(std::move(logging_level));
    }

    static void initialize(py::handle& m);

    explicit PyDatabase(const std::string& databasePath, uint64_t bufferPoolSize);

    void resizeBufferManager(uint64_t newSize);

    py::array_t<int64_t> scanNodeTableAsInt64(
        const std::string& tableName, const std::string& propName, py::list indices);

    py::array_t<double_t> scanNodeTableAsDouble(
        const std::string& tableName, const std::string& propName, py::list indices);

    py::array_t<bool> scanNodeTableAsBool(
        const std::string& tableName, const std::string& propName, py::list indices);

    template<class T>
    py::array_t<T> scanNodeTable(
        const std::string& tableName, const std::string& propName, py::list indices);

    ~PyDatabase() = default;

private:
    std::unique_ptr<Database> database;
};
