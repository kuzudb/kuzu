#pragma once

#include "transaction.h"

namespace kuzu {
namespace main {
class Database;
}

namespace transaction {

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
enum class TransactionMode : uint8_t { AUTO = 0, MANUAL = 1 };

class TransactionContext {
public:
    TransactionContext(main::Database* database);
    ~TransactionContext();

    inline bool isAutoTransaction() const { return mode == TransactionMode::AUTO; }

    void beginReadTransaction();
    void beginWriteTransaction();
    void beginAutoTransaction(bool readOnlyStatement);
    void validateManualTransaction(bool allowActiveTransaction, bool readOnlyStatement);

    void commit();
    void rollback();
    void commitSkipCheckPointing();
    void rollbackSkipCheckPointing();

    inline TransactionMode getTransactionMode() const { return mode; }
    inline bool hasActiveTransaction() const { return activeTransaction != nullptr; }
    inline Transaction* getActiveTransaction() const { return activeTransaction.get(); }

private:
    void commitInternal(bool skipCheckPointing);
    void rollbackInternal(bool skipCheckPointing);

private:
    void beginTransactionInternal(TransactionType transactionType);

private:
    std::mutex mtx;
    main::Database* database;
    TransactionMode mode;
    std::unique_ptr<Transaction> activeTransaction;
};

} // namespace transaction
} // namespace kuzu
