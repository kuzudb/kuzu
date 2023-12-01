#include "planner/operator/logical_transaction.h"
#include "processor/operator/transaction.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapTransaction(LogicalOperator* logicalOperator) {
    auto logicalTransaction = reinterpret_cast<LogicalTransaction*>(logicalOperator);
    return std::make_unique<Transaction>(logicalTransaction->getTransactionAction(),
        getOperatorID(), logicalTransaction->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
