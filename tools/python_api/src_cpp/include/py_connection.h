#pragma once

#include "py_database.h"
#include "py_query_result.h"

class PyConnection {

public:
    static void initialize(py::handle& m);

    explicit PyConnection(PyDatabase* pyDatabase, uint64_t numThreads);

    ~PyConnection() = default;

    unique_ptr<PyQueryResult> execute(const string& query, py::list params);

    void setMaxNumThreadForExec(uint64_t numThreads);

    py::str getNodePropertyNames(const string& tableName);

private:
    unordered_map<string, shared_ptr<Value>> transformPythonParameters(py::list params);

    pair<string, shared_ptr<Value>> transformPythonParameter(py::tuple param);

    Value transformPythonValue(py::handle val);

private:
    unique_ptr<Connection> conn;
};
