#include "transaction/transaction_context.h"

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
            activeTransaction.get());
    }
}

void TransactionContext::beginReadTransaction() {
    std::unique_lock lck{mtx};
    mode = TransactionMode::MANUAL;
    beginTransactionInternal(TransactionType::READ_ONLY);
}

void TransactionContext::beginWriteTransaction() {
    std::unique_lock lck{mtx};
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

void TransactionContext::beginRecoveryTransaction() {
    std::unique_lock lck{mtx};
    mode = TransactionMode::MANUAL;
    beginTransactionInternal(TransactionType::RECOVERY);
}

void TransactionContext::validateManualTransaction(bool readOnlyStatement) const {
    KU_ASSERT(hasActiveTransaction());
    if (activeTransaction->isReadOnly() && !readOnlyStatement) {
        throw TransactionManagerException(
            "Can not execute a write query inside a read-only transaction.");
    }
}

void TransactionContext::commit() {
    if (!hasActiveTransaction()) {
        return;
    }
    clientContext.getDatabase()->transactionManager->commit(clientContext);
    clearTransaction();
}

void TransactionContext::rollback() {
    if (!hasActiveTransaction()) {
        return;
    }
    clientContext.getDatabase()->transactionManager->rollback(clientContext,
        activeTransaction.get());
    clearTransaction();
}

void TransactionContext::clearTransaction() {
    activeTransaction = nullptr;
    mode = TransactionMode::AUTO;
}

void TransactionContext::beginTransactionInternal(TransactionType transactionType) {
    KU_ASSERT(!activeTransaction);
    activeTransaction = clientContext.getDatabase()->transactionManager->beginTransaction(
        clientContext, transactionType);
}

} // namespace transaction
} // namespace kuzu
