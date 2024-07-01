#include "planner/operator/logical_transaction.h"
#include "processor/operator/transaction.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapTransaction(LogicalOperator* logicalOperator) {
    auto& logicalTransaction = logicalOperator->constCast<LogicalTransaction>();
    auto printInfo = std::make_unique<OPPrintInfo>(logicalTransaction.getExpressionsForPrinting());
    return std::make_unique<Transaction>(logicalTransaction.getTransactionAction(), getOperatorID(),
        std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
