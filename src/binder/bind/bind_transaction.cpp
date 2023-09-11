#include "binder/binder.h"
#include "binder/bound_transaction_statement.h"
#include "parser/transaction_statement.h"

using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindTransaction(const Statement& statement) {
    auto transactionStatement = reinterpret_cast<const TransactionStatement&>(statement);
    return std::make_unique<BoundTransactionStatement>(transactionStatement.getTransactionAction());
}

} // namespace binder
} // namespace kuzu
