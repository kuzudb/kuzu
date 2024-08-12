#include "processor/operator/transaction.h"

#include "common/exception/transaction_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace processor {

std::string TransactionPrintInfo::toString() const {
    std::string result = "Action: ";
    result += TransactionActionUtils::toString(action);
    return result;
}

bool Transaction::getNextTuplesInternal(ExecutionContext* context) {
    if (hasExecuted) {
        return false;
    }
    hasExecuted = true;
    auto clientContext = context->clientContext;
    validateActiveTransaction(*clientContext->getTransactionContext());
    switch (transactionAction) {
    case TransactionAction::BEGIN_READ: {
        clientContext->getTransactionContext()->beginReadTransaction();
    } break;
    case TransactionAction::BEGIN_WRITE: {
        clientContext->getTransactionContext()->beginWriteTransaction();
    } break;
    case TransactionAction::COMMIT: {
        clientContext->getTransactionContext()->commit();
    } break;
    case TransactionAction::ROLLBACK: {
        clientContext->getTransactionContext()->rollback();
    } break;
    case TransactionAction::CHECKPOINT: {
        clientContext->getTransactionManagerUnsafe()->checkpoint(*clientContext);
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
    case TransactionAction::ROLLBACK: {
        if (!context.hasActiveTransaction()) {
            throw TransactionManagerException(stringFormat("No active transaction for {}.",
                TransactionActionUtils::toString(transactionAction)));
        }
    } break;
    case TransactionAction::CHECKPOINT: {
        if (context.hasActiveTransaction()) {
            throw TransactionManagerException(stringFormat("Found active transaction for {}.",
                TransactionActionUtils::toString(transactionAction)));
        }
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

} // namespace processor
} // namespace kuzu
