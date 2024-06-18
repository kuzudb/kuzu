#include "planner/operator/scan/logical_index_look_up.h"
#include "processor/operator/index_lookup.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapIndexLookup(LogicalOperator* logicalOperator) {
    auto& logicalIndexScan = logicalOperator->constCast<LogicalPrimaryKeyLookup>();
    auto outSchema = logicalIndexScan.getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto storageManager = clientContext->getStorageManager();
    std::vector<std::unique_ptr<IndexLookupInfo>> indexLookupInfos;
    for (auto i = 0u; i < logicalIndexScan.getNumInfos(); ++i) {
        auto& info = logicalIndexScan.getInfo(i);
        auto storageIndex =
            storageManager->getTable(info.nodeTableID)->ptrCast<storage::NodeTable>()->getPKIndex();
        auto offsetPos = DataPos(outSchema->getExpressionPos(*info.offset));
        auto keyPos = DataPos(outSchema->getExpressionPos(*info.key));
        indexLookupInfos.push_back(
            std::make_unique<IndexLookupInfo>(storageIndex, keyPos, offsetPos));
    }
    auto printInfo = std::make_unique<OPPrintInfo>(logicalIndexScan.getExpressionsForPrinting());
    return std::make_unique<IndexLookup>(std::move(indexLookupInfos), std::move(prevOperator),
        getOperatorID(), std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
