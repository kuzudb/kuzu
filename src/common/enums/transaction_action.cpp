#include "transaction/transaction_action.h"

#include "common/assert.h"

namespace kuzu {
namespace transaction {

std::string TransactionActionUtils::toString(TransactionAction action) {
    switch (action) {
    case TransactionAction::BEGIN_READ: {
        return "BEGIN_READ";
    }
    case TransactionAction::BEGIN_WRITE: {
        return "BEGIN_WRITE";
    }
    case TransactionAction::COMMIT: {
        return "COMMIT";
    }
    case TransactionAction::COMMIT_SKIP_CHECKPOINTING: {
        return "COMMIT_SKIP_CHECKPOINTING";
    }
    case TransactionAction::ROLLBACK: {
        return "ROLLBACK";
    }
    case TransactionAction::ROLLBACK_SKIP_CHECKPOINTING: {
        return "ROLLBACK_SKIP_CHECKPOINTING";
    }
    default:
        KU_UNREACHABLE;
    }
}

} // namespace transaction
} // namespace kuzu
