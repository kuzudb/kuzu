#include "binder/bound_transaction_statement.h"
#include "common/cast.h"
#include "planner/operator/logical_transaction.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

std::unique_ptr<LogicalPlan> Planner::planTransaction(const BoundStatement& statement) {
    auto& transactionStatement =
        common::ku_dynamic_cast<const BoundStatement&, const BoundTransactionStatement&>(statement);
    auto logicalTransaction =
        std::make_shared<LogicalTransaction>(transactionStatement.getTransactionAction());
    return getSimplePlan(std::move(logicalTransaction));
}

} // namespace planner
} // namespace kuzu
