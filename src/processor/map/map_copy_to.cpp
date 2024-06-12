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
    std::vector<std::unique_ptr<LogicalType>> columnTypes;
    for (auto& type : logicalCopyTo.getColumnTypesRef()) {
        columnTypes.push_back(std::make_unique<LogicalType>(type));
    }
    auto childSchema = logicalOperator->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<DataPos> vectorsToCopyPos;
    std::vector<bool> isFlat;
    for (auto& expression : childSchema->getExpressionsInScope()) {
        vectorsToCopyPos.emplace_back(childSchema->getExpressionPos(*expression));
        isFlat.push_back(childSchema->getGroup(expression)->isFlat());
    }
    auto copyFunc = logicalCopyTo.getCopyFunc();
    auto bindData = logicalCopyTo.getBindData();
    bindData->bindDataPos(childSchema, std::move(vectorsToCopyPos), std::move(isFlat));
    auto sharedState = copyFunc.copyToInitShared(*clientContext, *bindData);
    auto copyTo = std::make_unique<CopyTo>(std::make_unique<ResultSetDescriptor>(childSchema),
        copyFunc, std::move(bindData), std::move(sharedState), std::move(prevOperator),
        getOperatorID(), logicalCopyTo.getExpressionsForPrinting());

    //    if (copy->getFileType() == common::FileType::PARQUET) {
    //        copyTo =
    //        std::make_unique<CopyToParquet>(std::make_unique<ResultSetDescriptor>(childSchema),
    //            std::move(copyToInfo), std::move(sharedState), std::move(prevOperator),
    //            getOperatorID(), copy->getExpressionsForPrinting());
    //    } else {
    //        copyTo =
    //        std::make_unique<CopyToCSV>(std::make_unique<ResultSetDescriptor>(childSchema),
    //            std::move(copyToInfo), std::move(sharedState), std::move(prevOperator),
    //            getOperatorID(), copy->getExpressionsForPrinting());
    //    }
    auto fTable = std::make_shared<FactorizedTable>(clientContext->getMemoryManager(),
        FactorizedTableSchema());
    return createEmptyFTableScan(fTable, 0, std::move(copyTo));
}

} // namespace processor
} // namespace kuzu
