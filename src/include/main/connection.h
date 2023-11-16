#pragma once

#include <mutex>

#include "client_context.h"
#include "database.h"
#include "function/udf_function.h"
#include "prepared_statement.h"
#include "query_result.h"

namespace kuzu {
namespace main {

/**
 * @brief Connection is used to interact with a Database instance. Each Connection is thread-safe.
 * Multiple connections can connect to the same Database instance in a multi-threaded environment.
 */
class Connection {
    friend class kuzu::testing::BaseGraphTest;
    friend class kuzu::testing::PrivateGraphTest;
    friend class kuzu::testing::TestHelper;
    friend class kuzu::testing::TestRunner;
    friend class kuzu::benchmark::Benchmark;
    friend class kuzu::testing::TinySnbDDLTest;

public:
    /**
     * @brief Creates a connection to the database.
     * @param database A pointer to the database instance that this connection will be connected to.
     */
    KUZU_API explicit Connection(Database* database);
    /**
     * @brief Destructs the connection.
     */
    KUZU_API ~Connection();
    /**
     * @brief Manually starts a new read-only transaction in the current connection.
     */
    KUZU_API void beginReadOnlyTransaction();
    /**
     * @brief Manually starts a new write transaction in the current connection.
     */
    KUZU_API void beginWriteTransaction();
    /**
     * @brief Manually commits the current transaction.
     */
    KUZU_API void commit();
    /**
     * @brief Manually rollbacks the current transaction.
     */
    KUZU_API void rollback();
    /**
     * @brief Sets the maximum number of threads to use for execution in the current connection.
     * @param numThreads The number of threads to use for execution in the current connection.
     */
    KUZU_API void setMaxNumThreadForExec(uint64_t numThreads);
    /**
     * @brief Returns the maximum number of threads to use for execution in the current connection.
     * @return the maximum number of threads to use for execution in the current connection.
     */
    KUZU_API uint64_t getMaxNumThreadForExec();

    /**
     * @brief Executes the given query and returns the result.
     * @param query The query to execute.
     * @return the result of the query.
     */
    KUZU_API std::unique_ptr<QueryResult> query(const std::string& query);
    /**
     * @brief Prepares the given query and returns the prepared statement.
     * @param query The query to prepare.
     * @return the prepared statement.
     */
    KUZU_API std::unique_ptr<PreparedStatement> prepare(const std::string& query);
    /**
     * @brief Executes the given prepared statement with args and returns the result.
     * @param preparedStatement The prepared statement to execute.
     * @param args The parameter pack where each arg is a std::pair with the first element being
     * parameter name and second element being parameter value.
     * @return the result of the query.
     */
    template<typename... Args>
    inline std::unique_ptr<QueryResult> execute(
        PreparedStatement* preparedStatement, std::pair<std::string, Args>... args) {
        std::unordered_map<std::string, std::unique_ptr<common::Value>> inputParameters;
        return executeWithParams(preparedStatement, std::move(inputParameters), args...);
    }
    /**
     * @brief Executes the given prepared statement with inputParams and returns the result.
     * @param preparedStatement The prepared statement to execute.
     * @param inputParams The parameter pack where each arg is a std::pair with the first element
     * being parameter name and second element being parameter value.
     * @return the result of the query.
     */
    KUZU_API std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::unique_ptr<common::Value>> inputParams);
    /**
     * @brief interrupts all queries currently executing within this connection.
     */
    KUZU_API void interrupt();

    /**
     * @brief sets the query timeout value of the current connection. A value of zero (the default)
     * disables the timeout.
     */
    KUZU_API void setQueryTimeOut(uint64_t timeoutInMS);

    /**
     * @brief gets the query timeout value of the current connection. A value of zero (the default)
     * disables the timeout.
     */
    KUZU_API uint64_t getQueryTimeOut();

    template<typename TR, typename... Args>
    void createScalarFunction(const std::string& name, TR (*udfFunc)(Args...)) {
        addScalarFunction(name, std::move(function::UDF::getFunction<TR, Args...>(name, udfFunc)));
    }

    template<typename TR, typename... Args>
    void createScalarFunction(const std::string& name,
        std::vector<common::LogicalTypeID> parameterTypes, common::LogicalTypeID returnType,
        TR (*udfFunc)(Args...)) {
        addScalarFunction(name, function::UDF::getFunction<TR, Args...>(
                                    name, udfFunc, std::move(parameterTypes), returnType));
    }

    template<typename TR, typename... Args>
    void createVectorizedFunction(const std::string& name, function::scalar_exec_func scalarFunc) {
        addScalarFunction(
            name, function::UDF::getVectorizedFunction<TR, Args...>(name, std::move(scalarFunc)));
    }

    void createVectorizedFunction(const std::string& name,
        std::vector<common::LogicalTypeID> parameterTypes, common::LogicalTypeID returnType,
        function::scalar_exec_func scalarFunc) {
        addScalarFunction(name, function::UDF::getVectorizedFunction(name, std::move(scalarFunc),
                                    std::move(parameterTypes), returnType));
    }

    inline void setReplaceFunc(replace_func_t replaceFunc) {
        clientContext->setReplaceFunc(std::move(replaceFunc));
    }

private:
    std::unique_ptr<QueryResult> query(const std::string& query, const std::string& encodedJoin);

    std::unique_ptr<QueryResult> queryResultWithError(const std::string& errMsg);

    std::unique_ptr<PreparedStatement> prepareNoLock(const std::string& query,
        bool enumerateAllPlans = false, std::string joinOrder = std::string{});

    template<typename T, typename... Args>
    std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::unique_ptr<common::Value>> params,
        std::pair<std::string, T> arg, std::pair<std::string, Args>... args) {
        auto name = arg.first;
        auto val = std::make_unique<common::Value>((T)arg.second);
        params.insert({name, std::move(val)});
        return executeWithParams(preparedStatement, std::move(params), args...);
    }

    void bindParametersNoLock(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::unique_ptr<common::Value>> inputParams);

    std::unique_ptr<QueryResult> executeAndAutoCommitIfNecessaryNoLock(
        PreparedStatement* preparedStatement, uint32_t planIdx = 0u);

    KUZU_API void addScalarFunction(std::string name, function::function_set definitions);

    void checkPreparedStatementAccessMode(PreparedStatement* preparedStatement);

private:
    Database* database;
    std::unique_ptr<ClientContext> clientContext;
    std::mutex mtx;
};

} // namespace main
} // namespace kuzu
