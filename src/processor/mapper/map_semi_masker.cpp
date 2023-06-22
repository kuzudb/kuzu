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
    auto semiMasker = (LogicalSemiMasker*)logicalOperator;
    auto inSchema = semiMasker->getChild(0)->getSchema();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto node = semiMasker->getNode();
    std::unordered_map<common::table_id_t, std::vector<SemiMaskerInfo::mask_with_idx>>
        masksPerTable;
    for (auto tableID : node->getTableIDs()) {
        masksPerTable.insert({tableID, std::vector<SemiMaskerInfo::mask_with_idx>{}});
    }
    for (auto& op : semiMasker->getOperators()) {
        auto physicalOp = logicalOpToPhysicalOpMap.at(op);
        switch (physicalOp->getOperatorType()) {
        case PhysicalOperatorType::SCAN_NODE_ID: {
            auto scanNodeID = (ScanNodeID*)physicalOp;
            for (auto i = 0u; i < scanNodeID->getSharedState()->getNumTableStates(); ++i) {
                auto tableState = scanNodeID->getSharedState()->getTableState(i);
                auto tableID = tableState->getTable()->getTableID();
                masksPerTable.at(tableID).emplace_back(
                    tableState->getSemiMask(), 0 /* initial mask idx */);
            }
        } break;
        case PhysicalOperatorType::RECURSIVE_JOIN: {
            auto recursiveJoin = (RecursiveJoin*)physicalOp;
            for (auto& semiMask : recursiveJoin->getSharedState()->semiMasks) {
                auto tableID = semiMask->getNodeTable()->getTableID();
                masksPerTable.at(tableID).emplace_back(semiMask.get(), 0 /* initial mask idx */);
            }
        } break;
        default:
            throw common::NotImplementedException("PlanMapper::mapLogicalSemiMaskerToPhysical");
        }
    }
    auto keyPos = DataPos(inSchema->getExpressionPos(*semiMasker->getKey()));
    auto info = std::make_unique<SemiMaskerInfo>(keyPos, std::move(masksPerTable));
    switch (semiMasker->getType()) {
    case planner::SemiMaskType::NODE: {
        if (node->isMultiLabeled()) {
            return std::make_unique<MultiTableSemiMasker>(std::move(info), std::move(prevOperator),
                getOperatorID(), semiMasker->getExpressionsForPrinting());
        } else {
            return std::make_unique<SingleTableSemiMasker>(std::move(info), std::move(prevOperator),
                getOperatorID(), semiMasker->getExpressionsForPrinting());
        }
    }
    case planner::SemiMaskType::PATH: {
        if (node->isMultiLabeled()) {
            return std::make_unique<PathMultipleTableSemiMasker>(std::move(info),
                std::move(prevOperator), getOperatorID(), semiMasker->getExpressionsForPrinting());
        } else {
            return std::make_unique<PathSingleTableSemiMasker>(std::move(info),
                std::move(prevOperator), getOperatorID(), semiMasker->getExpressionsForPrinting());
        }
    }
    default:
        throw common::NotImplementedException("PlanMapper::mapLogicalSemiMaskerToPhysical");
    }
}

} // namespace processor
} // namespace kuzu
