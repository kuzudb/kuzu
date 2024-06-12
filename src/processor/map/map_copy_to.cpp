#include "planner/operator/persistent/logical_copy_to.h"
#include "processor/operator/persistent/copy_to.h"
#include "processor/plan_mapper.h"
#include "processor/result/factorized_table_util.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyTo(LogicalOperator* logicalOperator) {
    auto& logicalCopyTo = logicalOperator->constCast<LogicalCopyTo>();
    auto childSchema = logicalOperator->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<DataPos> vectorsToCopyPos;
    std::vector<bool> isFlat;
    std::vector<common::LogicalType> types;
    for (auto& expression : childSchema->getExpressionsInScope()) {
        vectorsToCopyPos.emplace_back(childSchema->getExpressionPos(*expression));
        isFlat.push_back(childSchema->getGroup(expression)->isFlat());
        types.push_back(expression->dataType);
    }
    auto copyFunc = logicalCopyTo.getCopyFunc();
    auto bindData = logicalCopyTo.getBindData();
    bindData->bindDataType(std::move(types));
    auto sharedState = copyFunc.copyToInitShared(*clientContext, *bindData);
    auto info = std::make_unique<CopyToInfo>(copyFunc, std::move(bindData),
        std::move(vectorsToCopyPos), std::move(isFlat));
    auto copyTo = std::make_unique<CopyTo>(std::make_unique<ResultSetDescriptor>(childSchema),
        std::move(info), std::move(sharedState), std::move(prevOperator), getOperatorID(),
        logicalCopyTo.getExpressionsForPrinting());
    auto fTable = std::make_shared<FactorizedTable>(clientContext->getMemoryManager(),
        FactorizedTableSchema());
    return createEmptyFTableScan(fTable, 0, std::move(copyTo));
}

} // namespace processor
} // namespace kuzu
