#pragma once

#include "py_database.h"
#include "py_query_result.h"

class PyConnection {

public:
    static void initialize(py::handle& m);

    explicit PyConnection(PyDatabase* pyDatabase);

    ~PyConnection() = default;

    unique_ptr<PyQueryResult> query(const string& query);

private:
    unique_ptr<Connection> conn;
};
