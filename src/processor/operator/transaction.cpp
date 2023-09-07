#include "processor/operator/transaction.h"

#include "common/exception/not_implemented.h"
#include "transaction/transaction_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

bool Transaction::getNextTuplesInternal(ExecutionContext* context) {
    if (hasExecuted) {
        return false;
    }
    hasExecuted = true;
    auto transactionContext = context->clientContext->getTransactionContext();
    switch (transactionAction) {
    case transaction::TransactionAction::BEGIN_READ: {
        transactionContext->beginReadTransaction();
    } break;
    case transaction::TransactionAction::BEGIN_WRITE: {
        transactionContext->beginWriteTransaction();
    } break;
    case transaction::TransactionAction::COMMIT: {
        transactionContext->commit();
    } break;
    case transaction::TransactionAction::COMMIT_SKIP_CHECKPOINTING: {
        transactionContext->commitSkipCheckPointing();
    } break;
    case transaction::TransactionAction::ROLLBACK: {
        transactionContext->rollback();
    } break;
    case transaction::TransactionAction::ROLLBACK_SKIP_CHECKPOINTING: {
        transactionContext->rollbackSkipCheckPointing();
    } break;
    default:                                                                 // LCOV_EXCL_START
        throw NotImplementedException("Transaction::getNextTuplesInternal"); // LCOV_EXCL_STOP
    }
    return true;
}

} // namespace processor
} // namespace kuzu
