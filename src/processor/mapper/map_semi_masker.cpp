#include "planner/logical_plan/logical_operator/logical_semi_masker.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/recursive_extend/recursive_join.h"
#include "processor/operator/scan_node_id.h"
#include "processor/operator/semi_masker.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSemiMaskerToPhysical(
    LogicalOperator* logicalOperator) {
    auto logicalSemiMasker = (LogicalSemiMasker*)logicalOperator;
    auto inSchema = logicalSemiMasker->getChild(0)->getSchema();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto node = logicalSemiMasker->getNode();
    auto keyDataPos = DataPos(inSchema->getExpressionPos(*node->getInternalIDProperty()));
    std::unordered_map<common::table_id_t, std::vector<mask_with_idx>> masksPerTable;
    for (auto tableID : node->getTableIDs()) {
        masksPerTable.insert({tableID, std::vector<mask_with_idx>{}});
    }
    for (auto& op : logicalSemiMasker->getOperators()) {
        auto physicalOp = logicalOpToPhysicalOpMap.at(op);
        switch (physicalOp->getOperatorType()) {
        case PhysicalOperatorType::SCAN_NODE_ID: {
            auto scanNodeID = (ScanNodeID*)physicalOp;
            for (auto i = 0u; i < scanNodeID->getSharedState()->getNumTableStates(); ++i) {
                auto tableState = scanNodeID->getSharedState()->getTableState(i);
                auto tableID = tableState->getTable()->getTableID();
                masksPerTable.at(tableID).emplace_back(tableState->getSemiMask(), 0);
            }
        } break;
        case PhysicalOperatorType::RECURSIVE_JOIN: {
            auto recursiveJoin = (BaseRecursiveJoin*)physicalOp;
            assert(!node->isMultiLabeled());
            auto tableID = node->getSingleTableID();
            masksPerTable.at(tableID).emplace_back(recursiveJoin->getSemiMask(), 0);
        } break;
        default:
            throw common::NotImplementedException("PlanMapper::mapLogicalSemiMaskerToPhysical");
        }
    }
    if (node->isMultiLabeled()) {
        return std::make_unique<MultiTableSemiMasker>(keyDataPos, std::move(masksPerTable),
            std::move(prevOperator), getOperatorID(),
            logicalSemiMasker->getExpressionsForPrinting());
    } else {
        return std::make_unique<SingleTableSemiMasker>(keyDataPos, std::move(masksPerTable),
            std::move(prevOperator), getOperatorID(),
            logicalSemiMasker->getExpressionsForPrinting());
    }
}

} // namespace processor
} // namespace kuzu
