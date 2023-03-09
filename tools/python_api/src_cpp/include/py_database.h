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

    ~PyDatabase() = default;

private:
    std::unique_ptr<Database> database;
};
