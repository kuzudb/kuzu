#pragma once

#include "py_database.h"
#include "py_query_result.h"

class PyConnection {

public:
    static void initialize(py::handle& m);

    explicit PyConnection(PyDatabase* pyDatabase, uint64_t numThreads);

    ~PyConnection() = default;

    std::unique_ptr<PyQueryResult> execute(const std::string& query, py::list params);

    void setMaxNumThreadForExec(uint64_t numThreads);

    py::str getNodePropertyNames(const std::string& tableName);

private:
    std::unordered_map<std::string, std::shared_ptr<kuzu::common::Value>> transformPythonParameters(
        py::list params);

    std::pair<std::string, std::shared_ptr<kuzu::common::Value>> transformPythonParameter(
        py::tuple param);

    kuzu::common::Value transformPythonValue(py::handle val);

private:
    std::unique_ptr<Connection> conn;
};
