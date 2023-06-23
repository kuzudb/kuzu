#pragma once

#include "client_context.h"
#include "database.h"
#include "prepared_statement.h"
#include "query_result.h"

namespace kuzu {
namespace main {

/**
 * @brief Connection is used to interact with a Database instance. Each Connection is thread-safe.
 * Multiple connections can connect to the same Database instance in a multi-threaded environment.
 */
class Connection {
    friend class kuzu::testing::ApiTest;
    friend class kuzu::testing::BaseGraphTest;
    friend class kuzu::testing::TestHelper;
    friend class kuzu::testing::TestRunner;
    friend class kuzu::benchmark::Benchmark;

public:
    /**
     * If the connection is in AUTO_COMMIT mode any query over the connection will be wrapped around
     * a transaction and committed (even if the query is READ_ONLY).
     * If the connection is in MANUAL transaction mode, which happens only if an application
     * manually begins a transaction (see below), then an application has to manually commit or
     * rollback the transaction by calling commit() or rollback().
     *
     * AUTO_COMMIT is the default mode when a Connection is created. If an application calls
     * begin[ReadOnly/Write]Transaction at any point, the mode switches to MANUAL. This creates
     * an "active transaction" in the connection. When a connection is in MANUAL mode and the
     * active transaction is rolled back or committed, then the active transaction is removed (so
     * the connection no longer has an active transaction) and the mode automatically switches
     * back to AUTO_COMMIT.
     * Note: When a Connection object is deconstructed, if the connection has an active (manual)
     * transaction, then the active transaction is rolled back.
     */
    enum class ConnectionTransactionMode : uint8_t { AUTO_COMMIT = 0, MANUAL = 1 };

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
    KUZU_API template<typename... Args>
    inline std::unique_ptr<QueryResult> execute(
        PreparedStatement* preparedStatement, std::pair<std::string, Args>... args) {
        std::unordered_map<std::string, std::shared_ptr<common::Value>> inputParameters;
        return executeWithParams(preparedStatement, inputParameters, args...);
    }
    /**
     * @brief Executes the given prepared statement with inputParams and returns the result.
     * @param preparedStatement The prepared statement to execute.
     * @param inputParams The parameter pack where each arg is a std::pair with the first element
     * being parameter name and second element being parameter value.
     * @return the result of the query.
     */
    KUZU_API std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::shared_ptr<common::Value>>& inputParams);
    /**
     * @return all node table names in string format.
     */
    KUZU_API std::string getNodeTableNames();
    /**
     * @return all rel table names in string format.
     */
    KUZU_API std::string getRelTableNames();
    /**
     * @param nodeTableName The name of the node table.
     * @return all property names of the given table.
     */
    KUZU_API std::string getNodePropertyNames(const std::string& tableName);
    /**
     * @param relTableName The name of the rel table.
     * @return all property names of the given table.
     */
    KUZU_API std::string getRelPropertyNames(const std::string& relTableName);

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

protected:
    ConnectionTransactionMode getTransactionMode();
    void setTransactionModeNoLock(ConnectionTransactionMode newTransactionMode);

    std::unique_ptr<QueryResult> query(const std::string& query, const std::string& encodedJoin);
    // Note: This is only added for testing recovery algorithms in unit tests. Do not use
    // this otherwise.
    void commitButSkipCheckpointingForTestingRecovery();
    // Note: This is only added for testing recovery algorithms in unit tests. Do not use
    // this otherwise.
    void rollbackButSkipCheckpointingForTestingRecovery();
    // Note: This is only added for testing recovery algorithms in unit tests. Do not use
    // this otherwise.
    transaction::Transaction* getActiveTransaction();
    // used in API test

    uint64_t getActiveTransactionID();
    bool hasActiveTransaction();
    void commitNoLock();
    void rollbackIfNecessaryNoLock();

    void beginTransactionNoLock(transaction::TransactionType type);

    void commitOrRollbackNoLock(
        transaction::TransactionAction action, bool skipCheckpointForTesting = false);

    std::unique_ptr<QueryResult> queryResultWithError(std::string& errMsg);

    std::unique_ptr<PreparedStatement> prepareNoLock(const std::string& query,
        bool enumerateAllPlans = false, std::string joinOrder = std::string{});

    template<typename T, typename... Args>
    std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::shared_ptr<common::Value>>& params,
        std::pair<std::string, T> arg, std::pair<std::string, Args>... args) {
        auto name = arg.first;
        auto val = std::make_shared<common::Value>((T)arg.second);
        params.insert({name, val});
        return executeWithParams(preparedStatement, params, args...);
    }

    void bindParametersNoLock(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::shared_ptr<common::Value>>& inputParams);

    std::unique_ptr<QueryResult> executeAndAutoCommitIfNecessaryNoLock(
        PreparedStatement* preparedStatement, uint32_t planIdx = 0u);

    void beginTransactionIfAutoCommit(PreparedStatement* preparedStatement);

private:
    inline std::unique_ptr<QueryResult> getQueryResultWithError(std::string exceptionMessage) {
        rollbackIfNecessaryNoLock();
        return queryResultWithError(exceptionMessage);
    }

protected:
    Database* database;
    std::unique_ptr<ClientContext> clientContext;
    std::unique_ptr<transaction::Transaction> activeTransaction;
    ConnectionTransactionMode transactionMode;
    std::mutex mtx;
};

} // namespace main
} // namespace kuzu
