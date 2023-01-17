#pragma once

#include "py_database.h"
#include "py_query_result.h"

class PyConnection {

public:
    static void initialize(py::handle& m);

    explicit PyConnection(PyDatabase* pyDatabase, uint64_t numThreads);

    ~PyConnection() = default;

    unique_ptr<PyQueryResult> execute(const string& query, const py::list& params);

    void setMaxNumThreadForExec(uint64_t numThreads);

private:
    unordered_map<string, shared_ptr<Value>> transformPythonParameters(const py::list& params);

    pair<string, shared_ptr<Value>> transformPythonParameter(const py::tuple& param);

    Value transformPythonValue(py::handle val);

private:
    unique_ptr<Connection> conn;
};
