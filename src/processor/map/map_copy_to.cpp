#include "planner/operator/persistent/logical_copy_to.h"
#include "processor/operator/persistent/copy_to.h"
#include "processor/plan_mapper.h"

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
        types.push_back(expression->dataType.copy());
    }
    auto exportFunc = logicalCopyTo.getExportFunc();
    auto bindData = logicalCopyTo.getBindData();
    // TODO(Xiyang): Query: COPY (RETURN null) TO '/tmp/1.parquet', the datatype of the first
    // column is ANY, should we solve the type at binder?
    bindData->setDataType(std::move(types));
    auto sharedState = exportFunc.initShared(*clientContext, *bindData);
    auto info =
        CopyToInfo{exportFunc, std::move(bindData), std::move(vectorsToCopyPos), std::move(isFlat)};
    auto printInfo = std::make_unique<OPPrintInfo>(logicalCopyTo.getExpressionsForPrinting());
    auto copyTo = std::make_unique<CopyTo>(std::make_unique<ResultSetDescriptor>(childSchema),
        std::move(info), std::move(sharedState), std::move(prevOperator), getOperatorID(),
        std::move(printInfo));
    auto fTable = std::make_shared<FactorizedTable>(clientContext->getMemoryManager(),
        FactorizedTableSchema());
    return createEmptyFTableScan(fTable, 0, std::move(copyTo));
}

} // namespace processor
} // namespace kuzu
