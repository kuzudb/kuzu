#pragma once

#include "py_database.h"
#include "py_prepared_statement.h"
#include "py_query_result.h"

class PyConnection {

public:
    static void initialize(py::handle& m);

    explicit PyConnection(PyDatabase* pyDatabase, uint64_t numThreads);

    ~PyConnection() = default;

    void setQueryTimeout(uint64_t timeoutInMS);

    std::unique_ptr<PyQueryResult> execute(PyPreparedStatement* preparedStatement, py::list params);

    void setMaxNumThreadForExec(uint64_t numThreads);

    py::str getNodePropertyNames(const std::string& tableName);

    py::str getNodeTableNames();

    py::str getRelPropertyNames(const std::string& tableName);

    py::str getRelTableNames();

    PyPreparedStatement prepare(const std::string& query);

    py::array_t<int64_t> getAllEdgesForTorchGeometric(const std::string& srcTableName,
        const std::string& relName, const std::string& dstTableName, size_t queryBatchSize);

private:
    std::unordered_map<std::string, std::shared_ptr<kuzu::common::Value>> transformPythonParameters(
        py::list params);

    std::pair<std::string, std::shared_ptr<kuzu::common::Value>> transformPythonParameter(
        py::tuple param);

    kuzu::common::Value transformPythonValue(py::handle val);

private:
    std::unique_ptr<Connection> conn;
};
