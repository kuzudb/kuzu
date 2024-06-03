#include "planner/operator/scan/logical_index_look_up.h"
#include "processor/operator/index_lookup.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapIndexScan(
    planner::LogicalOperator* logicalOperator) {
    auto logicalIndexScan = (LogicalPrimaryKeyLookup*)logicalOperator;
    auto outSchema = logicalIndexScan->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    // TODO(Xiyang): potentially we should merge IndexScan and IndexLook
    auto storageManager = clientContext->getStorageManager();
    if (logicalIndexScan->getNumInfos() > 1) {
        std::vector<std::unique_ptr<IndexLookupInfo>> indexLookupInfos;
        for (auto i = 0u; i < logicalIndexScan->getNumInfos(); ++i) {
            auto& info = logicalIndexScan->getInfo(i);
            auto storageIndex = common::ku_dynamic_cast<storage::Table*, storage::NodeTable*>(
                storageManager->getTable(info.nodeTableID))
                                    ->getPKIndex();
            auto offsetPos = DataPos(outSchema->getExpressionPos(*info.offset));
            auto keyPos = DataPos(outSchema->getExpressionPos(*info.key));
            indexLookupInfos.push_back(
                std::make_unique<IndexLookupInfo>(storageIndex, keyPos, offsetPos));
        }
        return std::make_unique<IndexLookup>(std::move(indexLookupInfos), std::move(prevOperator),
            getOperatorID(), logicalIndexScan->getExpressionsForPrinting());
    }
    KU_UNREACHABLE;
}

} // namespace processor
} // namespace kuzu
