#include "planner/operator/scan/logical_index_scan.h"
#include "processor/operator/index_lookup.h"
#include "processor/operator/index_scan.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapIndexScan(
    planner::LogicalOperator* logicalOperator) {
    auto logicalIndexScan = (LogicalIndexScanNode*)logicalOperator;
    auto inSchema = logicalIndexScan->getChild(0)->getSchema();
    auto outSchema = logicalIndexScan->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    // TODO(Xiyang): potentially we should merge IndexScan and IndexLook
    if (logicalIndexScan->getNumInfos() > 1) {
        std::vector<std::unique_ptr<IndexLookupInfo>> indexLookupInfos;
        for (auto i = 0u; i < logicalIndexScan->getNumInfos(); ++i) {
            auto info = logicalIndexScan->getInfo(i);
            auto storageIndex = storageManager.getNodeTable(info->nodeTableID)->getPKIndex();
            auto offsetPos = DataPos(outSchema->getExpressionPos(*info->offset));
            auto keyPos = DataPos(outSchema->getExpressionPos(*info->key));
            indexLookupInfos.push_back(std::make_unique<IndexLookupInfo>(
                info->nodeTableID, info->indexType->copy(), storageIndex, keyPos, offsetPos));
        }
        return std::make_unique<IndexLookup>(std::move(indexLookupInfos), std::move(prevOperator),
            getOperatorID(), logicalIndexScan->getExpressionsForPrinting());
    } else {
        auto info = logicalIndexScan->getInfo(0);
        auto nodeTable = storageManager.getNodeTable(info->nodeTableID);
        auto evaluator = ExpressionMapper::getEvaluator(info->key, inSchema);
        auto outDataPos = DataPos(outSchema->getExpressionPos(*info->offset));
        return make_unique<IndexScan>(nodeTable->getTableID(), nodeTable->getPKIndex(),
            std::move(evaluator), outDataPos, std::move(prevOperator), getOperatorID(),
            logicalIndexScan->getExpressionsForPrinting());
    }
}

} // namespace processor
} // namespace kuzu
