#include "transaction/transaction_context.h"

#include "common/exception/connection.h"
#include "common/exception/transaction_manager.h"
#include "main/client_context.h"
#include "main/database.h"
#include "transaction/transaction_manager.h"

using namespace kuzu::common;

namespace kuzu {
namespace transaction {

TransactionContext::TransactionContext(main::ClientContext& clientContext)
    : clientContext{clientContext}, mode{TransactionMode::AUTO} {}

TransactionContext::~TransactionContext() {
    if (activeTransaction) {
        clientContext.getDatabase()->transactionManager->rollback(clientContext,
            activeTransaction.get(), false /*skipCheckPointing*/);
    }
}

void TransactionContext::beginReadTransaction() {
    std::unique_lock<std::mutex> lck{mtx};
    mode = TransactionMode::MANUAL;
    beginTransactionInternal(TransactionType::READ_ONLY);
}

void TransactionContext::beginWriteTransaction() {
    std::unique_lock<std::mutex> lck{mtx};
    mode = TransactionMode::MANUAL;
    beginTransactionInternal(TransactionType::WRITE);
}

void TransactionContext::beginAutoTransaction(bool readOnlyStatement) {
    if (mode == TransactionMode::AUTO && hasActiveTransaction()) {
        activeTransaction.reset();
    }
    beginTransactionInternal(
        readOnlyStatement ? TransactionType::READ_ONLY : TransactionType::WRITE);
}

void TransactionContext::validateManualTransaction(bool allowActiveTransaction,
    bool readOnlyStatement) {
    KU_ASSERT(hasActiveTransaction());
    if (activeTransaction->isReadOnly() && !readOnlyStatement) {
        throw ConnectionException("Can not execute a write query inside a read-only transaction.");
    }
    if (!allowActiveTransaction) {
        throw ConnectionException(
            "DDL, Copy, createMacro statements can only run in the AUTO_COMMIT mode. Please commit "
            "or rollback your previous transaction if there is any and issue the query without "
            "beginning a transaction");
    }
}

void TransactionContext::commit() {
    commitInternal(false /* skipCheckPointing */);
}

void TransactionContext::rollback() {
    rollbackInternal(false /* skipCheckPointing */);
}

void TransactionContext::commitSkipCheckPointing() {
    commitInternal(true /* skipCheckPointing */);
}

void TransactionContext::rollbackSkipCheckPointing() {
    rollbackInternal(true /* skipCheckPointing */);
}

void TransactionContext::clearTransaction() {
    activeTransaction = nullptr;
    mode = TransactionMode::AUTO;
}

void TransactionContext::commitInternal(bool skipCheckPointing) {
    if (!hasActiveTransaction()) {
        return;
    }
    clientContext.getDatabase()->transactionManager->commit(clientContext, skipCheckPointing);
    clearTransaction();
}

void TransactionContext::rollbackInternal(bool skipCheckPointing) {
    if (!hasActiveTransaction()) {
        return;
    }
    clientContext.getDatabase()->transactionManager->rollback(clientContext,
        activeTransaction.get(), skipCheckPointing);
    clearTransaction();
}

void TransactionContext::beginTransactionInternal(TransactionType transactionType) {
    if (activeTransaction) {
        throw TransactionManagerException(
            "Connection already has an active transaction. Applications can have one "
            "transaction per connection at any point in time. For concurrent multiple "
            "transactions, please open other connections. Current active transaction is "
            "not affected by this exception and can still be used.");
    }
    activeTransaction = clientContext.getDatabase()->transactionManager->beginTransaction(
        clientContext, transactionType);
}

} // namespace transaction
} // namespace kuzu
