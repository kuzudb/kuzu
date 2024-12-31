#include "optimizer/reorder_extend_direction.h"

#include "planner/operator/extend/logical_extend.h"
#include "planner/operator/scan/logical_scan_node_table.h"

#include "storage/storage_manager.h"

using namespace kuzu::planner;
using namespace kuzu::binder;

namespace kuzu {
namespace optimizer {

void ReorderExtendDirection::rewrite(LogicalPlan* plan) {
    visitOperator(plan->getLastOperator());
}

std::shared_ptr<LogicalOperator> ReorderExtendDirection::visitOperator(
        const std::shared_ptr<LogicalOperator>& op) {
    // bottom-up traversal
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        op->setChild(i, visitOperator(op->getChild(i)));
    }
    auto result = visitOperatorReplaceSwitch(op);
    result->computeFlatSchema();
    return result;
}

static inline offset_t getNumNodes(main::ClientContext *context, std::vector<common::table_id_t> tableIds) {
    offset_t numNodes = 0;
    for (auto tableId: tableIds) {
        numNodes += context->getStorageManager()->getNodesStatisticsAndDeletedIDs()->getMaxNodeOffset(
                context->getTx(), tableId);
    }
    return numNodes;
}

static inline common::ExtendDirection reverseDirection(common::ExtendDirection direction) {
    return direction == common::ExtendDirection::FWD ? common::ExtendDirection::BWD : common::ExtendDirection::FWD;
}

std::shared_ptr<planner::LogicalOperator> ReorderExtendDirection::visitExtendReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
    auto logicalExtend = op->ptrCast<LogicalExtend>();
    if (logicalExtend->getDirection() == ExtendDirection::BOTH || logicalExtend->getPropertyPredicates().size() > 0) {
        return op;
    }

    if (logicalExtend->getChild(0)->getOperatorType() != LogicalOperatorType::SCAN_NODE_TABLE) {
        return op;
    }

    auto scanNode = logicalExtend->getChild(0)->ptrCast<LogicalScanNodeTable>();
    if (scanNode->getProperties().size() > 0 || scanNode->getExtraInfo() != nullptr || scanNode->getPropertyPredicates().size() > 0) {
        return op;
    }

    auto scanNodeCost = getNumNodes(context, logicalExtend->getBoundNode()->getTableIDs());
    auto lookupNodeCost = getNumNodes(context, logicalExtend->getNbrNode()->getTableIDs());
    if (scanNodeCost < lookupNodeCost) {
        return op;
    }

    // swap the order
    auto newScan = std::make_shared<LogicalScanNodeTable>(logicalExtend->getNbrNode()->getInternalID(),
                                                          logicalExtend->getNbrNode()->getTableIDs(),
                                                          expression_vector());
    newScan->setScanType(scanNode->getScanType());
    // Call computeFlatSchema to update the schema
    newScan->computeFlatSchema();
    auto reverseExtend = std::make_shared<LogicalExtend>(logicalExtend->getNbrNode(),
                                                         logicalExtend->getBoundNode(),
                                                         logicalExtend->getRel(),
                                                         reverseDirection(logicalExtend->getDirection()),
                                                         !logicalExtend->extendFromSourceNode(),
                                                         logicalExtend->getProperties(),
                                                         logicalExtend->getHasAtMostOneNbr(),
                                                         std::move(newScan));
    return reverseExtend;
}

} // namespace optimizer
} // namespace kuzu