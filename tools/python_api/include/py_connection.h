#pragma once

#include "py_database.h"
#include "py_query_result.h"

class PyConnection {

public:
    explicit PyConnection(PyDatabase* pyDatabase, uint64_t numThreads);
    ~PyConnection() = default;

    static void initialize(py::handle& m);

    unique_ptr<PyQueryResult> execute(const string& query, py::list params);

    void setMaxNumThreadForExec(uint64_t numThreads);

private:
    unordered_map<string, shared_ptr<Literal>> transformPythonParameters(py::list params);

    pair<string, shared_ptr<Literal>> transformPythonParameter(py::tuple param);

    Literal transformPythonValue(py::handle val);

private:
    unique_ptr<Connection> conn;
};
