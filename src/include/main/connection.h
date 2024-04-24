#pragma once

#include "client_context.h"
#include "database.h"
#include "function/udf_function.h"

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
     * @brief Sets the maximum number of threads to use for execution in the current connection.
     * @param numThreads The number of threads to use for execution in the current connection.
     */
    KUZU_API void setMaxNumThreadForExec(uint64_t numThreads);
    /**
     * @brief Returns the maximum number of threads to use for execution in the current connection.
     * @return the maximum number of threads to use for execution in the current connection.
     */
    KUZU_API uint64_t getMaxNumThreadForExec();

    void setProgressBarPrinting(bool enable) {
        clientContext->progressBar->toggleProgressBarPrinting(enable);
    }

    /**
     * @brief Executes the given query and returns the result.
     * @param query The query to execute.
     * @return the result of the query.
     */
    KUZU_API std::unique_ptr<QueryResult> query(std::string_view query);
    /**
     * @brief Prepares the given query and returns the prepared statement.
     * @param query The query to prepare.
     * @return the prepared statement.
     */
    KUZU_API std::unique_ptr<PreparedStatement> prepare(std::string_view query);
    /**
     * @brief Executes the given prepared statement with args and returns the result.
     * @param preparedStatement The prepared statement to execute.
     * @param args The parameter pack where each arg is a std::pair with the first element being
     * parameter name and second element being parameter value.
     * @return the result of the query.
     */
    template<typename... Args>
    inline std::unique_ptr<QueryResult> execute(PreparedStatement* preparedStatement,
        std::pair<std::string, Args>... args) {
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
    void createScalarFunction(std::string name, TR (*udfFunc)(Args...)) {
        auto autoTrx = startUDFAutoTrx(clientContext->getTransactionContext());
        auto nameCopy = std::string(name);
        addScalarFunction(std::move(nameCopy),
            function::UDF::getFunction<TR, Args...>(std::move(name), udfFunc));
        commitUDFTrx(autoTrx);
    }

    template<typename TR, typename... Args>
    void createScalarFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypes,
        common::LogicalTypeID returnType, TR (*udfFunc)(Args...)) {
        auto autoTrx = startUDFAutoTrx(clientContext->getTransactionContext());
        auto nameCopy = std::string(name);
        addScalarFunction(std::move(nameCopy),
            function::UDF::getFunction<TR, Args...>(std::move(name), udfFunc,
                std::move(parameterTypes), returnType));
        commitUDFTrx(autoTrx);
    }

    void addUDFFunctionSet(std::string name, function::function_set func) {
        auto autoTrx = startUDFAutoTrx(clientContext->getTransactionContext());
        addScalarFunction(std::move(name), std::move(func));
        commitUDFTrx(autoTrx);
    }

    template<typename TR, typename... Args>
    void createVectorizedFunction(std::string name, function::scalar_func_exec_t scalarFunc) {
        auto autoTrx = startUDFAutoTrx(clientContext->getTransactionContext());
        auto nameCopy = std::string(name);
        addScalarFunction(std::move(nameCopy), function::UDF::getVectorizedFunction<TR, Args...>(
                                                   std::move(name), std::move(scalarFunc)));
        commitUDFTrx(autoTrx);
    }

    void createVectorizedFunction(std::string name,
        std::vector<common::LogicalTypeID> parameterTypes, common::LogicalTypeID returnType,
        function::scalar_func_exec_t scalarFunc) {
        auto autoTrx = startUDFAutoTrx(clientContext->getTransactionContext());
        auto nameCopy = std::string(name);
        addScalarFunction(std::move(nameCopy),
            function::UDF::getVectorizedFunction(std::move(name), std::move(scalarFunc),
                std::move(parameterTypes), returnType));
        commitUDFTrx(autoTrx);
    }

    ClientContext* getClientContext() { return clientContext.get(); };

private:
    std::unique_ptr<QueryResult> query(std::string_view query, std::string_view encodedJoin,
        bool enumerateAllPlans = true);

    std::unique_ptr<QueryResult> queryResultWithError(std::string_view errMsg);

    std::unique_ptr<PreparedStatement> preparedStatementWithError(std::string_view errMsg);

    std::unique_ptr<PreparedStatement> prepareNoLock(
        std::shared_ptr<parser::Statement> parsedStatement, bool enumerateAllPlans = false,
        std::string_view joinOrder = std::string_view());

    template<typename T, typename... Args>
    std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::unique_ptr<common::Value>> params,
        std::pair<std::string, T> arg, std::pair<std::string, Args>... args) {
        return clientContext->executeWithParams(preparedStatement, std::move(params), arg, args...);
    }

    void bindParametersNoLock(PreparedStatement* preparedStatement,
        const std::unordered_map<std::string, std::unique_ptr<common::Value>>& inputParams);

    std::unique_ptr<QueryResult> executeAndAutoCommitIfNecessaryNoLock(
        PreparedStatement* preparedStatement, uint32_t planIdx = 0u);

    KUZU_API void addScalarFunction(std::string name, function::function_set definitions);

    KUZU_API bool startUDFAutoTrx(transaction::TransactionContext* trx);
    KUZU_API void commitUDFTrx(bool isAutoCommitTrx);

private:
    Database* database;
    std::unique_ptr<ClientContext> clientContext;
};

} // namespace main
} // namespace kuzu
