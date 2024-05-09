#include "processor/operator/transaction.h"

#include "common/exception/transaction_manager.h"
#include "transaction/transaction_context.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace processor {

bool Transaction::getNextTuplesInternal(ExecutionContext* context) {
    if (hasExecuted) {
        return false;
    }
    hasExecuted = true;
    auto transactionContext = context->clientContext->getTransactionContext();
    validateActiveTransaction(*transactionContext);
    switch (transactionAction) {
    case TransactionAction::BEGIN_READ: {
        transactionContext->beginReadTransaction();
    } break;
    case TransactionAction::BEGIN_WRITE: {
        transactionContext->beginWriteTransaction();
    } break;
    case TransactionAction::COMMIT: {
        transactionContext->commit();
    } break;
    case TransactionAction::COMMIT_SKIP_CHECKPOINTING: {
        transactionContext->commitSkipCheckPointing();
    } break;
    case TransactionAction::ROLLBACK: {
        transactionContext->rollback();
    } break;
    case TransactionAction::ROLLBACK_SKIP_CHECKPOINTING: {
        transactionContext->rollbackSkipCheckPointing();
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return true;
}

void Transaction::validateActiveTransaction(const TransactionContext& context) const {
    switch (transactionAction) {
    case TransactionAction::BEGIN_READ:
    case TransactionAction::BEGIN_WRITE: {
        if (context.hasActiveTransaction()) {
            throw TransactionManagerException(
                "Connection already has an active transaction. Cannot start a transaction within "
                "another one. For concurrent multiple transactions, please open other "
                "connections.");
        }
    } break;
    case TransactionAction::COMMIT:
    case TransactionAction::COMMIT_SKIP_CHECKPOINTING: {
        if (!context.hasActiveTransaction()) {
            throw TransactionManagerException("No active transaction for COMMIT.");
        }
    } break;
    case TransactionAction::ROLLBACK:
    case TransactionAction::ROLLBACK_SKIP_CHECKPOINTING: {
        if (!context.hasActiveTransaction()) {
            throw TransactionManagerException("No active transaction for ROLLBACK.");
        }
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

} // namespace processor
} // namespace kuzu
