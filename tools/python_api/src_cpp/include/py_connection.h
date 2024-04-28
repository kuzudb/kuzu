#pragma once

#include "main/storage_driver.h"
#include "py_database.h"
#include "py_prepared_statement.h"
#include "py_query_result.h"

using kuzu::common::LogicalType;
using kuzu::common::LogicalTypeID;
using kuzu::common::Value;

class PyConnection {

public:
    static void initialize(py::handle& m);

    explicit PyConnection(PyDatabase* pyDatabase, uint64_t numThreads);

    void close();

    ~PyConnection() = default;

    void setQueryTimeout(uint64_t timeoutInMS);

    std::unique_ptr<PyQueryResult> execute(PyPreparedStatement* preparedStatement,
        const py::dict& params);

    std::unique_ptr<PyQueryResult> query(const std::string& statement);

    void setMaxNumThreadForExec(uint64_t numThreads);

    PyPreparedStatement prepare(const std::string& query);

    uint64_t getNumNodes(const std::string& nodeName);

    uint64_t getNumRels(const std::string& relName);

    void getAllEdgesForTorchGeometric(py::array_t<int64_t>& npArray,
        const std::string& srcTableName, const std::string& relName,
        const std::string& dstTableName, size_t queryBatchSize);

    static bool isPandasDataframe(const py::object& object);

    void createScalarFunction(const std::string& name, const py::function& udf,
        const py::list& params, const std::string& retval);

    static Value transformPythonValue(const py::handle& val);
    static Value transformPythonValueAs(const py::handle& val, const LogicalType& type);

private:
    std::unique_ptr<StorageDriver> storageDriver;
    std::unique_ptr<Connection> conn;

    static std::unique_ptr<PyQueryResult> checkAndWrapQueryResult(
        std::unique_ptr<QueryResult>& queryResult);
};
