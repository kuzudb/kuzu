#include "common/exception/not_implemented.h"
#include "parser/transaction_statement.h"
#include "parser/transformer.h"

using namespace kuzu::transaction;
using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::unique_ptr<Statement> Transformer::transformTransaction(
    CypherParser::KU_TransactionContext& ctx) {
    if (ctx.TRANSACTION()) {
        if (ctx.READ()) {
            return std::make_unique<TransactionStatement>(TransactionAction::BEGIN_READ);
        } else {
            return std::make_unique<TransactionStatement>(TransactionAction::BEGIN_WRITE);
        }
    } else if (ctx.COMMIT()) {
        return std::make_unique<TransactionStatement>(TransactionAction::COMMIT);
    } else if (ctx.COMMIT_SKIP_CHECKPOINT()) {
        return std::make_unique<TransactionStatement>(TransactionAction::COMMIT_SKIP_CHECKPOINTING);
    } else if (ctx.ROLLBACK()) {
        return std::make_unique<TransactionStatement>(TransactionAction::ROLLBACK);
    } else if (ctx.ROLLBACK_SKIP_CHECKPOINT()) {
        return std::make_unique<TransactionStatement>(
            TransactionAction::ROLLBACK_SKIP_CHECKPOINTING);
    } else {                                                                // LCOV_EXCL_START
        throw NotImplementedException("Transformer::transformTransaction"); // LCOV_EXCL_STOP
    }
}

} // namespace parser
} // namespace kuzu
