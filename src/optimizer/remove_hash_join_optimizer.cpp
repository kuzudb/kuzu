#include "optimizer/remove_hash_join_optimizer.h"

#include "planner/operator/logical_hash_join.h"
#include "planner/operator/scan/logical_scan_node_table.h"

using namespace kuzu::planner;
using namespace kuzu::binder;

namespace kuzu {
namespace optimizer {

bool RemoveHashBuildValidator::validate(planner::LogicalOperator *op) {
    bool isValid = true;
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        isValid &= validate(op->getChild(i).get());
    }

    switch (op->getOperatorType()) {
        case LogicalOperatorType::FILTER:
        case LogicalOperatorType::PROJECTION: {
            isValid &= true;
        } break;
        case LogicalOperatorType::SCAN_NODE_TABLE: {
            isValid &= validateScanNodeTable(op);
        } break;
        default: {
            isValid = false;
        } break;
    }
    return isValid;
}

bool RemoveHashBuildValidator::validateScanNodeTable(planner::LogicalOperator *op) const {
    auto scanNodeTable = op->ptrCast<LogicalScanNodeTable>();
    auto tableIds = scanNodeTable->getTableIDs();
    if (tableIds.size() != 1) {
        return false;
    }
    return tableIds[0] == tableId;
}

bool NodeTableCollector::validateAndCollect(planner::LogicalOperator *op) {
    bool isValid = true;
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        isValid &= validateAndCollect(op->getChild(i).get());
    }
    switch (op->getOperatorType()) {
        case LogicalOperatorType::EXTEND:
        case LogicalOperatorType::PROJECTION:
        case LogicalOperatorType::FILTER: {
            isValid &= true;
        } break;
        case LogicalOperatorType::SCAN_NODE_TABLE: {
            isValid &= visitScanNodeTable(op);
        } break;
        default: {
            isValid = false;
        } break;
    }
    return isValid;
}

bool NodeTableCollector::visitScanNodeTable(planner::LogicalOperator *op) {
    auto scanNodeTable = op->ptrCast<LogicalScanNodeTable>();
    if (!scanNodeTable->getProperties().empty()) {
        return false;
    }
    auto tableIds = scanNodeTable->getTableIDs();
    if (tableIds.size() != 1) {
        return false;
    }
    tableId = tableIds[0];
    return true;
}

std::shared_ptr<planner::LogicalOperator> ReplaceNodeTableOnProbeSide::rewrite(
        const std::shared_ptr<planner::LogicalOperator> &op) {
    // bottom-up traversal
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        op->setChild(i, rewrite(op->getChild(i)));
    }
    auto result = visitOperatorReplaceSwitch(op);
    result->computeFlatSchema();
    return result;
}

std::shared_ptr<planner::LogicalOperator> ReplaceNodeTableOnProbeSide::visitScanNodeTableReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
    auto scanNodeTable = op->ptrCast<LogicalScanNodeTable>();
    auto tableIds = scanNodeTable->getTableIDs();
    if (tableIds.size() != 1) {
        return op;
    }
    if (tableIds[0] != tableId) {
        return op;
    }
    return replacement;
}

void RemoveHashJoinOptimizer::rewrite(planner::LogicalPlan *plan) {
    plan->setLastOperator(visitOperator(plan->getLastOperator()));
}

std::shared_ptr<planner::LogicalOperator> RemoveHashJoinOptimizer::visitOperator(
        const std::shared_ptr<planner::LogicalOperator> &op) {
    // bottom-up traversal
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        op->setChild(i, visitOperator(op->getChild(i)));
    }
    auto result = visitOperatorReplaceSwitch(op);
    result->computeFlatSchema();
    return result;
}

std::shared_ptr<planner::LogicalOperator> RemoveHashJoinOptimizer::visitHashJoinReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
    auto hashJoin = op->ptrCast<LogicalHashJoin>();
    auto probeSide = hashJoin->getChild(0);
    auto buildSide = hashJoin->getChild(1);

    auto probeCollector = NodeTableCollector();
    if (!probeCollector.validateAndCollect(probeSide.get())) {
        return op;
    }
    KU_ASSERT(probeCollector.tableId != common::INVALID_TABLE_ID);
    auto buildValidator = RemoveHashBuildValidator(probeCollector.tableId);
    if (!buildValidator.validate(buildSide.get())) {
        return op;
    }

    auto replacement = ReplaceNodeTableOnProbeSide(probeCollector.tableId, buildSide);
    replacement.rewrite(probeSide);
    return probeSide;
}

} // namespace optimizer
} // namespace kuzu