#pragma once

#include "../common/types/value.h"
#include "client_context.h"
#include "database.h"
#include "forward_declarations.h"
#include "prepared_statement.h"
#include "query_result.h"

using namespace kuzu::planner;
using namespace kuzu::transaction;
using lock_t = unique_lock<mutex>;

namespace kuzu {
namespace main {

class Connection {
    friend class kuzu::testing::ApiTest;
    friend class kuzu::testing::BaseGraphTest;
    friend class kuzu::testing::TestHelper;

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
    enum ConnectionTransactionMode : uint8_t { AUTO_COMMIT, MANUAL };

public:
    explicit Connection(Database* database);

    ~Connection();

    void beginReadOnlyTransaction();

    void beginWriteTransaction();

    void commit();

    void rollback();

    void setMaxNumThreadForExec(uint64_t numThreads);

    uint64_t getMaxNumThreadForExec();

    std::unique_ptr<QueryResult> query(const std::string& query);

    std::unique_ptr<PreparedStatement> prepare(const std::string& query);

    template<typename... Args>
    inline std::unique_ptr<QueryResult> execute(
        PreparedStatement* preparedStatement, pair<string, Args>... args) {
        unordered_map<string, shared_ptr<Value>> inputParameters;
        return executeWithParams(preparedStatement, inputParameters, args...);
    }
    // Note: Any call that goes through executeWithParams acquires a lock in the end by calling
    // executeLock(...).
    std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        unordered_map<string, shared_ptr<Value>>& inputParams);

    string getNodeTableNames();
    string getRelTableNames();
    string getNodePropertyNames(const string& tableName);
    string getRelPropertyNames(const string& relTableName);

protected:
    ConnectionTransactionMode getTransactionMode();
    void setTransactionModeNoLock(ConnectionTransactionMode newTransactionMode);
    // Note: This is only added for testing recovery algorithms in unit tests. Do not use
    // this otherwise.
    void commitButSkipCheckpointingForTestingRecovery();
    // Note: This is only added for testing recovery algorithms in unit tests. Do not use
    // this otherwise.
    void rollbackButSkipCheckpointingForTestingRecovery();
    // Note: This is only added for testing recovery algorithms in unit tests. Do not use
    // this otherwise.
    Transaction* getActiveTransaction();
    // used in API test

    uint64_t getActiveTransactionID();
    bool hasActiveTransaction();
    void commitNoLock();
    void rollbackIfNecessaryNoLock();

    void beginTransactionNoLock(TransactionType type);

    void commitOrRollbackNoLock(bool isCommit, bool skipCheckpointForTesting = false);

    unique_ptr<QueryResult> queryResultWithError(std::string& errMsg);

    std::unique_ptr<PreparedStatement> prepareNoLock(
        const std::string& query, bool enumerateAllPlans = false);

    template<typename T, typename... Args>
    std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        unordered_map<string, shared_ptr<Value>>& params, pair<string, T> arg,
        pair<string, Args>... args) {
        auto name = arg.first;
        auto val = make_shared<Value>(Value::createValue<T>(arg.second));
        params.insert({name, val});
        return executeWithParams(preparedStatement, params, args...);
    }

    void bindParametersNoLock(PreparedStatement* preparedStatement,
        unordered_map<string, shared_ptr<Value>>& inputParams);

    std::unique_ptr<QueryResult> executeAndAutoCommitIfNecessaryNoLock(
        PreparedStatement* preparedStatement, uint32_t planIdx = 0u);

    void beginTransactionIfAutoCommit(PreparedStatement* preparedStatement);

protected:
    Database* database;
    std::unique_ptr<ClientContext> clientContext;
    std::unique_ptr<Transaction> activeTransaction;
    ConnectionTransactionMode transactionMode;
    std::mutex mtx;
};

} // namespace main
} // namespace kuzu
