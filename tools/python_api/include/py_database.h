#pragma once

#include "pybind_include.h"

#include "src/main/include/graphflowdb.h"

using namespace graphflow::main;

class PyDatabase {
    friend class PyConnection;

public:
    static void initialize(py::handle& m);

    explicit PyDatabase(const string& databasePath);

    ~PyDatabase() = default;

private:
    unique_ptr<Database> database;
};
