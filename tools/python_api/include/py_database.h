#pragma once

#include "main/kuzu.h"
#include "pybind_include.h"

using namespace kuzu::main;

class PyDatabase {
    friend class PyConnection;

public:
    explicit PyDatabase(const string& databasePath, uint64_t bufferPoolSize);
    ~PyDatabase() = default;

    inline void setLoggingLevel(spdlog::level::level_enum logging_level) {
        database->setLoggingLevel(logging_level);
    }

    static void initialize(py::handle& m);

    void resizeBufferManager(uint64_t newSize);

private:
    unique_ptr<Database> database;
};
