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
        std::unique_lock<std::mutex> lck{mtx};
        setTransactionModeNoLock(MANUAL);
        beginTransactionNoLock(transaction::READ_ONLY);
    }

    inline void beginWriteTransaction() {
        std::unique_lock<std::mutex> lck{mtx};
        setTransactionModeNoLock(MANUAL);
        beginTransactionNoLock(transaction::WRITE);
    }

    inline void commit() {
        std::unique_lock<std::mutex> lck{mtx};
        commitOrRollbackNoLock(true /* isCommit */);
    }

    inline void rollback() {
        std::unique_lock<std::mutex> lck{mtx};
        commitOrRollbackNoLock(false /* is rollback */);
    }

    inline void setMaxNumThreadForExec(uint64_t numThreads) {
        clientContext->numThreadsForExecution = numThreads;
    }
    inline uint64_t getMaxNumThreadForExec() {
        std::unique_lock<std::mutex> lck{mtx};
        return clientContext->numThreadsForExecution;
    }

    std::unique_ptr<QueryResult> query(const std::string& query);

    std::unique_ptr<PreparedStatement> prepare(const std::string& query) {
        std::unique_lock<std::mutex> lck{mtx};
        return prepareNoLock(query);
    }

    template<typename... Args>
    inline std::unique_ptr<QueryResult> execute(
        PreparedStatement* preparedStatement, std::pair<std::string, Args>... args) {
        std::unordered_map<std::string, std::shared_ptr<common::Value>> inputParameters;
        return executeWithParams(preparedStatement, inputParameters, args...);
    }
    // Note: Any call that goes through executeWithParams acquires a lock in the end by calling
    // executeLock(...).
    std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::shared_ptr<common::Value>>& inputParams);

    std::string getNodeTableNames();
    std::string getRelTableNames();
    std::string getNodePropertyNames(const std::string& tableName);
    std::string getRelPropertyNames(const std::string& relTableName);

protected:
    inline ConnectionTransactionMode getTransactionMode() {
        std::unique_lock<std::mutex> lck{mtx};
        return transactionMode;
    }
    inline void setTransactionModeNoLock(ConnectionTransactionMode newTransactionMode) {
        if (activeTransaction && transactionMode == MANUAL && newTransactionMode == AUTO_COMMIT) {
            throw common::ConnectionException(
                "Cannot change transaction mode from MANUAL to AUTO_COMMIT when there is an "
                "active transaction. Need to first commit or rollback the active transaction.");
        }
        transactionMode = newTransactionMode;
    }
    // Note: This is only added for testing recovery algorithms in unit tests. Do not use
    // this otherwise.
    inline void commitButSkipCheckpointingForTestingRecovery() {
        std::unique_lock<std::mutex> lck{mtx};
        commitOrRollbackNoLock(true /* isCommit */, true /* skip checkpointing for testing */);
    }
    // Note: This is only added for testing recovery algorithms in unit tests. Do not use
    // this otherwise.
    inline void rollbackButSkipCheckpointingForTestingRecovery() {
        std::unique_lock<std::mutex> lck{mtx};
        commitOrRollbackNoLock(false /* is rollback */, true /* skip checkpointing for testing */);
    }
    // Note: This is only added for testing recovery algorithms in unit tests. Do not use
    // this otherwise.
    inline transaction::Transaction* getActiveTransaction() {
        std::unique_lock<std::mutex> lck{mtx};
        return activeTransaction.get();
    }
    // used in API test

    inline uint64_t getActiveTransactionID() {
        std::unique_lock<std::mutex> lck{mtx};
        return activeTransaction ? activeTransaction->getID() : UINT64_MAX;
    }
    inline bool hasActiveTransaction() {
        std::unique_lock<std::mutex> lck{mtx};
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

    void beginTransactionNoLock(transaction::TransactionType type);

    void commitOrRollbackNoLock(bool isCommit, bool skipCheckpointForTesting = false);

    std::unique_ptr<QueryResult> queryResultWithError(std::string& errMsg);

    std::unique_ptr<PreparedStatement> prepareNoLock(
        const std::string& query, bool enumerateAllPlans = false);

    template<typename T, typename... Args>
    std::unique_ptr<QueryResult> executeWithParams(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::shared_ptr<common::Value>>& params,
        std::pair<std::string, T> arg, std::pair<std::string, Args>... args) {
        auto name = arg.first;
        auto val = std::make_shared<common::Value>(common::Value::createValue<T>(arg.second));
        params.insert({name, val});
        return executeWithParams(preparedStatement, params, args...);
    }

    void bindParametersNoLock(PreparedStatement* preparedStatement,
        std::unordered_map<std::string, std::shared_ptr<common::Value>>& inputParams);

    std::unique_ptr<QueryResult> executeAndAutoCommitIfNecessaryNoLock(
        PreparedStatement* preparedStatement, uint32_t planIdx = 0u);

    void beginTransactionIfAutoCommit(PreparedStatement* preparedStatement);

protected:
    Database* database;
    std::unique_ptr<ClientContext> clientContext;
    std::unique_ptr<transaction::Transaction> activeTransaction;
    ConnectionTransactionMode transactionMode;
    std::mutex mtx;
};

} // namespace main
} // namespace kuzu
