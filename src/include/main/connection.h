#pragma once

#include "binder/binder.h"
#include "catalog/catalog.h"
#include "client_context.h"
#include "main/database.h"
#include "planner/logical_plan/logical_plan.h"
#include "prepared_statement.h"
#include "query_result.h"
#include "storage/wal/wal.h"
#include "transaction/transaction_manager.h"

using namespace kuzu::planner;
using namespace kuzu::transaction;
using lock_t = unique_lock<mutex>;

namespace kuzu {
namespace testing {
class ApiTest;
class BaseGraphTest;
class TestHelper;
} // namespace testing
} // namespace kuzu

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

    inline void beginReadOnlyTransaction() {
        lock_t lck{mtx};
        setTransactionModeNoLock(MANUAL);
        beginTransactionNoLock(READ_ONLY);
    }

    inline void beginWriteTransaction() {
        lock_t lck{mtx};
        setTransactionModeNoLock(MANUAL);
        beginTransactionNoLock(WRITE);
    }

    inline void commit() {
        lock_t lck{mtx};
        commitOrRollbackNoLock(true /* isCommit */);
    }

    inline void rollback() {
        lock_t lck{mtx};
        commitOrRollbackNoLock(false /* is rollback */);
    }

    inline void setMaxNumThreadForExec(uint64_t numThreads) {
        clientContext->numThreadsForExecution = numThreads;
    }
    inline uint64_t getMaxNumThreadForExec() {
        lock_t lck{mtx};
        return clientContext->numThreadsForExecution;
    }

    std::unique_ptr<QueryResult> query(const std::string& query);

    std::unique_ptr<PreparedStatement> prepare(const std::string& query) {
        lock_t lck{mtx};
        return prepareNoLock(query);
    }

    template<typename... Args>
    inline std::unique_ptr<QueryResult> execute(
        PreparedStatement* preparedStatement, pair<string, Args>... args) {
        unordered_map<string, shared_ptr<Literal>> inputParameters;
        return executeWithParams(preparedStatement, inputParameters, args...);
    }
    // Note: Any call that goes through executeWithParams acquires a lock in the end by calling
    // executeLock(...).
    std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        unordered_map<string, shared_ptr<Literal>>& inputParams);

    string getNodeTableNames();
    string getRelTableNames();
    string getNodePropertyNames(const string& tableName);
    string getRelPropertyNames(const string& relTableName);

protected:
    inline ConnectionTransactionMode getTransactionMode() {
        lock_t lck{mtx};
        return transactionMode;
    }
    inline void setTransactionModeNoLock(ConnectionTransactionMode newTransactionMode) {
        if (activeTransaction && transactionMode == MANUAL && newTransactionMode == AUTO_COMMIT) {
            throw ConnectionException(
                "Cannot change transaction mode from MANUAL to AUTO_COMMIT when there is an "
                "active transaction. Need to first commit or rollback the active transaction.");
        }
        transactionMode = newTransactionMode;
    }
    // Note: This is only added for testing recovery algorithms in unit tests. Do not use
    // this otherwise.
    inline void commitButSkipCheckpointingForTestingRecovery() {
        lock_t lck{mtx};
        commitOrRollbackNoLock(true /* isCommit */, true /* skip checkpointing for testing */);
    }
    // Note: This is only added for testing recovery algorithms in unit tests. Do not use
    // this otherwise.
    inline void rollbackButSkipCheckpointingForTestingRecovery() {
        lock_t lck{mtx};
        commitOrRollbackNoLock(false /* is rollback */, true /* skip checkpointing for testing */);
    }
    // Note: This is only added for testing recovery algorithms in unit tests. Do not use
    // this otherwise.
    inline Transaction* getActiveTransaction() {
        lock_t lck{mtx};
        return activeTransaction.get();
    }
    // used in API test

    inline uint64_t getActiveTransactionID() {
        lock_t lck{mtx};
        return activeTransaction ? activeTransaction->getID() : UINT64_MAX;
    }
    inline bool hasActiveTransaction() {
        lock_t lck{mtx};
        return activeTransaction != nullptr;
    }
    inline void commitNoLock() { commitOrRollbackNoLock(true /* is commit */); }
    inline void rollbackIfNecessaryNoLock() {
        // If there is no active transaction in the system (e.g., an exception occurs during
        // planning), we shouldn't roll back the transaction.
        if (activeTransaction != nullptr) {
            commitOrRollbackNoLock(false /* is rollback */);
        }
    }

    // Used in test helper. Note: for our testing framework, we should not catch exception and
    // instead let IDE catch these exceptions.
    std::vector<unique_ptr<planner::LogicalPlan>> enumeratePlans(const std::string& query);
    unique_ptr<planner::LogicalPlan> getBestPlan(const std::string& query);
    std::unique_ptr<QueryResult> executePlan(unique_ptr<planner::LogicalPlan> logicalPlan);

    void beginTransactionNoLock(TransactionType type);

    void commitOrRollbackNoLock(bool isCommit, bool skipCheckpointForTesting = false);

    unique_ptr<QueryResult> queryResultWithError(std::string& errMsg);

    void setQuerySummaryAndPreparedStatement(
        Statement* statement, Binder& binder, PreparedStatement* preparedStatement);

    std::unique_ptr<PreparedStatement> prepareNoLock(const std::string& query);

    template<typename T, typename... Args>
    std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        unordered_map<string, shared_ptr<Literal>>& params, pair<string, T> arg,
        pair<string, Args>... args) {
        auto name = arg.first;
        auto val = make_shared<Literal>(Literal::createLiteral<T>(arg.second));
        params.insert({name, val});
        return executeWithParams(preparedStatement, params, args...);
    }

    void bindParametersNoLock(PreparedStatement* preparedStatement,
        unordered_map<string, shared_ptr<Literal>>& inputParams);

    std::unique_ptr<QueryResult> executeAndAutoCommitIfNecessaryNoLock(
        PreparedStatement* preparedStatement);

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
