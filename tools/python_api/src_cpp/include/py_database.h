#pragma once

#include "main/kuzu.h"
#include "pybind_include.h"

using namespace kuzu::main;

class PyDatabase {
    friend class PyConnection;

public:
    inline void setLoggingLevel(spdlog::level::level_enum logging_level) {
        database->setLoggingLevel(logging_level);
    }

    static void initialize(py::handle& m);

    explicit PyDatabase(const string& databasePath, uint64_t bufferPoolSize);

    void resizeBufferManager(uint64_t newSize);

    ~PyDatabase() = default;

private:
    unique_ptr<Database> database;
};
