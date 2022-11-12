#pragma once

#include "pybind_include.h"

#include "src/main/include/kuzu.h"

using namespace kuzu::main;

class PyDatabase {
    friend class PyConnection;

public:
    static void initialize(py::handle& m);

    explicit PyDatabase(const string& databasePath, uint64_t bufferPoolSize);

    void resizeBufferManager(uint64_t newSize);

    ~PyDatabase() = default;

private:
    unique_ptr<Database> database;
};
