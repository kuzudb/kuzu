#include "planner/operator/sip/logical_semi_masker.h"
#include "processor/operator/recursive_extend/recursive_join.h"
#include "processor/operator/scan_node_id.h"
#include "processor/operator/semi_masker.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapSemiMasker(LogicalOperator* logicalOperator) {
    auto semiMasker = (LogicalSemiMasker*)logicalOperator;
    auto inSchema = semiMasker->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto tableIDs = semiMasker->getNodeTableIDs();
    std::unordered_map<table_id_t, std::vector<SemiMaskerInfo::mask_with_idx>> masksPerTable;
    for (auto tableID : tableIDs) {
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
                masksPerTable.at(tableID).emplace_back(tableState->getSemiMask(),
                    0 /* initial mask idx */);
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
            KU_UNREACHABLE;
        }
    }
    auto keyPos = DataPos(inSchema->getExpressionPos(*semiMasker->getKey()));
    auto info = std::make_unique<SemiMaskerInfo>(keyPos, std::move(masksPerTable));
    switch (semiMasker->getType()) {
    case planner::SemiMaskType::NODE: {
        if (tableIDs.size() > 1) {
            return std::make_unique<MultiTableSemiMasker>(std::move(info), std::move(prevOperator),
                getOperatorID(), semiMasker->getExpressionsForPrinting());
        } else {
            return std::make_unique<SingleTableSemiMasker>(std::move(info), std::move(prevOperator),
                getOperatorID(), semiMasker->getExpressionsForPrinting());
        }
    }
    case planner::SemiMaskType::PATH: {
        if (tableIDs.size() > 1) {
            return std::make_unique<PathMultipleTableSemiMasker>(std::move(info),
                std::move(prevOperator), getOperatorID(), semiMasker->getExpressionsForPrinting());
        } else {
            return std::make_unique<PathSingleTableSemiMasker>(std::move(info),
                std::move(prevOperator), getOperatorID(), semiMasker->getExpressionsForPrinting());
        }
    }
    default:
        KU_UNREACHABLE;
    }
}

} // namespace processor
} // namespace kuzu
