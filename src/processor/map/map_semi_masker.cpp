#include "planner/operator/sip/logical_semi_masker.h"
#include "processor/operator/gds_call.h"
#include "processor/operator/scan/scan_node_table.h"
#include "processor/operator/semi_masker.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

// masksPerTable is collected from semiMasker.
// maskPerTable is collected from target operator, i.e. GDS, scan, ...
// Normally the two maps should have the same tableIDs.
// An exception is in GDS with filtered projected graph, multiple semiMasker will work on
// the same target GDS operator so masksPerTable may have fewer tableIDs.
static void initMask(table_id_map_t<std::vector<semi_mask_t*>>& masksPerTable,
    const table_id_map_t<semi_mask_t*>& maskPerTable) {
    for (auto& [tableID, masks] : masksPerTable) {
        KU_ASSERT(maskPerTable.contains(tableID));
        auto mask = maskPerTable.at(tableID);
        mask->enable();
        masks.emplace_back(mask);
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapSemiMasker(
    const LogicalOperator* logicalOperator) {
    const auto& semiMasker = logicalOperator->constCast<LogicalSemiMasker>();
    const auto inSchema = semiMasker.getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    const auto tableIDs = semiMasker.getNodeTableIDs();
    table_id_map_t<std::vector<semi_mask_t*>> masksPerTable;
    for (auto tableID : tableIDs) {
        masksPerTable.insert({tableID, std::vector<semi_mask_t*>{}});
    }
    std::vector<std::string> operatorNames;
    for (auto& op : semiMasker.getTargetOperators()) {
        const auto physicalOp = logicalOpToPhysicalOpMap.at(op);
        operatorNames.push_back(PhysicalOperatorUtils::operatorToString(physicalOp));
        switch (physicalOp->getOperatorType()) {
        case PhysicalOperatorType::SCAN_NODE_TABLE: {
            KU_ASSERT(semiMasker.getTargetType() == SemiMaskTargetType::SCAN_NODE);
            auto scan = physicalOp->ptrCast<ScanNodeTable>();
            initMask(masksPerTable, scan->getSemiMasks());
        } break;
        case PhysicalOperatorType::GDS_CALL: {
            auto sharedState = physicalOp->ptrCast<GDSCall>()->getSharedState();
            switch (semiMasker.getTargetType()) {
            case SemiMaskTargetType::RECURSIVE_EXTEND_INPUT_NODE: {
                initMask(masksPerTable, sharedState->getInputNodeMaskMap()->getMasks());
                sharedState->enableInputNodeMask();
            } break;
            case SemiMaskTargetType::RECURSIVE_EXTEND_OUTPUT_NODE: {
                initMask(masksPerTable, sharedState->getOutputNodeMaskMap()->getMasks());
                sharedState->enableOutputNodeMask();
            } break;
            case SemiMaskTargetType::RECURSIVE_EXTEND_PATH_NODE: {
                initMask(masksPerTable, sharedState->getPathNodeMaskMap()->getMasks());
            } break;
            case SemiMaskTargetType::GDS_GRAPH_NODE: {
                initMask(masksPerTable, sharedState->getGraphNodeMaskMap()->getMasks());
            } break;
            default:
                KU_UNREACHABLE;
            }
        } break;
        default:
            KU_UNREACHABLE;
        }
    }
    auto keyPos = DataPos(inSchema->getExpressionPos(*semiMasker.getKey()));
    auto sharedState = std::make_shared<SemiMaskerSharedState>(std::move(masksPerTable));
    auto printInfo = std::make_unique<SemiMaskerPrintInfo>(operatorNames);
    switch (semiMasker.getKeyType()) {
    case SemiMaskKeyType::NODE: {
        if (tableIDs.size() > 1) {
            return std::make_unique<MultiTableSemiMasker>(keyPos, sharedState,
                std::move(prevOperator), getOperatorID(), std::move(printInfo));
        } else {
            return std::make_unique<SingleTableSemiMasker>(keyPos, sharedState,
                std::move(prevOperator), getOperatorID(), std::move(printInfo));
        }
    }
    case SemiMaskKeyType::PATH: {
        auto& extraInfo = semiMasker.getExtraKeyInfo()->constCast<ExtraPathKeyInfo>();
        if (tableIDs.size() > 1) {
            return std::make_unique<PathMultipleTableSemiMasker>(keyPos, sharedState,
                std::move(prevOperator), getOperatorID(), std::move(printInfo),
                extraInfo.direction);
        } else {
            return std::make_unique<PathSingleTableSemiMasker>(keyPos, sharedState,
                std::move(prevOperator), getOperatorID(), std::move(printInfo),
                extraInfo.direction);
        }
    }
    case SemiMaskKeyType::NODE_ID_LIST: {
        auto& extraInfo = semiMasker.getExtraKeyInfo()->constCast<ExtraNodeIDListKeyInfo>();
        auto srcIDPos = getDataPos(*extraInfo.srcNodeID, *inSchema);
        auto dstIDPos = getDataPos(*extraInfo.dstNodeID, *inSchema);
        if (tableIDs.size() > 1) {
            return std::make_unique<NodeIDsMultipleTableSemiMasker>(keyPos, srcIDPos, dstIDPos,
                sharedState, std::move(prevOperator), getOperatorID(), std::move(printInfo));
        } else {
            return std::make_unique<NodeIDsSingleTableSemiMasker>(keyPos, srcIDPos, dstIDPos,
                sharedState, std::move(prevOperator), getOperatorID(), std::move(printInfo));
        }
    }
    default:
        KU_UNREACHABLE;
    }
}

} // namespace processor
} // namespace kuzu
