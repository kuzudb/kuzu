#pragma once

#include "py_database.h"
#include "py_query_result.h"

class PyConnection {

public:
    static void initialize(py::handle& m);

    explicit PyConnection(PyDatabase* pyDatabase);

    ~PyConnection() = default;

    unique_ptr<PyQueryResult> execute(const string& query, py::list params);

private:
    unordered_map<string, shared_ptr<Literal>> transformPythonParameters(py::list params);

    pair<string, shared_ptr<Literal>> transformPythonParameter(py::tuple param);

    Literal transformPythonValue(py::handle val);

private:
    unique_ptr<Connection> conn;
};
