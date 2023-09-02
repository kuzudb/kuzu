#include "planner/logical_plan/copy/logical_copy_to.h"
#include "processor/operator/persistent/copy_to.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyTo(LogicalOperator* logicalOperator) {
    auto copy = (LogicalCopyTo*)logicalOperator;
    auto childSchema = logicalOperator->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<DataPos> vectorsToCopyPos;
    for (auto& expression : childSchema->getExpressionsInScope()) {
        vectorsToCopyPos.emplace_back(childSchema->getExpressionPos(*expression));
    }
    auto sharedState = std::make_shared<WriteCSVFileSharedState>();
    return std::make_unique<CopyTo>(sharedState, std::move(vectorsToCopyPos),
        *copy->getCopyDescription(), getOperatorID(), copy->getExpressionsForPrinting(),
        std::move(prevOperator));
}

} // namespace processor
} // namespace kuzu
