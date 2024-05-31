#include "planner/operator/sip/logical_semi_masker.h"
#include "processor/operator/gds_call.h"
#include "processor/operator/recursive_extend/recursive_join.h"
#include "processor/operator/scan/scan_node_table.h"
#include "processor/operator/semi_masker.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static void initMaskIdx(common::table_id_map_t<std::vector<mask_with_idx>>& masksPerTable,
    std::vector<NodeSemiMask*> masks) {
    for (auto& mask : masks) {
        auto tableID = mask->getTableID();
        masksPerTable.at(tableID).emplace_back(mask, 0 /* initial mask idx */);
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapSemiMasker(LogicalOperator* logicalOperator) {
    const auto& semiMasker = logicalOperator->cast<LogicalSemiMasker>();
    const auto inSchema = semiMasker.getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    const auto tableIDs = semiMasker.getNodeTableIDs();
    common::table_id_map_t<std::vector<mask_with_idx>> masksPerTable;
    for (auto tableID : tableIDs) {
        masksPerTable.insert({tableID, std::vector<mask_with_idx>{}});
    }
    for (auto& op : semiMasker.getOperators()) {
        const auto physicalOp = logicalOpToPhysicalOpMap.at(op);
        switch (physicalOp->getOperatorType()) {
        case PhysicalOperatorType::SCAN_NODE_TABLE: {
            auto scan = physicalOp->ptrCast<ScanNodeTable>();
            initMaskIdx(masksPerTable, scan->getSemiMasks());
        } break;
        case PhysicalOperatorType::RECURSIVE_JOIN: {
            auto& recursiveJoin = physicalOp->constCast<RecursiveJoin>();
            initMaskIdx(masksPerTable, recursiveJoin.getSemiMask());
        } break;
        case PhysicalOperatorType::TABLE_FUNCTION_CALL: {
            KU_ASSERT(physicalOp->getChild(0)->getOperatorType() == PhysicalOperatorType::GDS_CALL);
            auto gds = physicalOp->getChild(0)->ptrCast<GDSCall>();
            KU_ASSERT(gds->hasSemiMask());
            initMaskIdx(masksPerTable, gds->getSemiMasks());
        } break;
        default:
            KU_UNREACHABLE;
        }
    }
    auto keyPos = DataPos(inSchema->getExpressionPos(*semiMasker.getKey()));
    auto info = std::make_unique<SemiMaskerInfo>(keyPos, std::move(masksPerTable));
    auto printInfo = std::make_unique<OPPrintInfo>(semiMasker.getExpressionsForPrinting());
    switch (semiMasker.getType()) {
    case planner::SemiMaskConstructionType::NODE: {
        if (tableIDs.size() > 1) {
            return std::make_unique<MultiTableSemiMasker>(std::move(info), std::move(prevOperator),
                getOperatorID(), std::move(printInfo));
        } else {
            return std::make_unique<SingleTableSemiMasker>(std::move(info), std::move(prevOperator),
                getOperatorID(), std::move(printInfo));
        }
    }
    case planner::SemiMaskConstructionType::PATH: {
        if (tableIDs.size() > 1) {
            return std::make_unique<PathMultipleTableSemiMasker>(std::move(info),
                std::move(prevOperator), getOperatorID(), std::move(printInfo));
        } else {
            return std::make_unique<PathSingleTableSemiMasker>(std::move(info),
                std::move(prevOperator), getOperatorID(), std::move(printInfo));
        }
    }
    default:
        KU_UNREACHABLE;
    }
}

} // namespace processor
} // namespace kuzu
