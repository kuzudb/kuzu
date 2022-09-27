#include "include/logical_plan_util.h"

#include "src/planner/logical_plan/logical_operator/include/logical_extend.h"
#include "src/planner/logical_plan/logical_operator/include/logical_hash_join.h"
#include "src/planner/logical_plan/logical_operator/include/logical_scan_node_id.h"

namespace graphflow {
namespace planner {

vector<LogicalOperator*> LogicalPlanUtil::collectOperators(
    LogicalOperator* root, LogicalOperatorType operatorType) {
    vector<LogicalOperator*> result;
    collectOperatorsRecursive(root, operatorType, result);
    return result;
}

void LogicalPlanUtil::encodeJoinRecursive(LogicalOperator* logicalOperator, string& encodeString) {
    switch (logicalOperator->getLogicalOperatorType()) {
    case LogicalOperatorType::LOGICAL_HASH_JOIN: {
        encodeHashJoin(logicalOperator, encodeString);
        encodeString += "{";
        encodeJoinRecursive(logicalOperator->getChild(0).get(), encodeString);
        encodeString += "}{";
        encodeJoinRecursive(logicalOperator->getChild(1).get(), encodeString);
        encodeString += "}";
    } break;
    case LogicalOperatorType::LOGICAL_EXTEND: {
        encodeExtend(logicalOperator, encodeString);
        encodeJoinRecursive(logicalOperator->getChild(0).get(), encodeString);
    } break;
    case LogicalOperatorType::LOGICAL_SCAN_NODE_ID: {
        encodeScanNodeID(logicalOperator, encodeString);
    } break;
    default:
        for (auto i = 0u; i < logicalOperator->getNumChildren(); ++i) {
            encodeJoinRecursive(logicalOperator->getChild(i).get(), encodeString);
        }
    }
}

void LogicalPlanUtil::collectOperatorsRecursive(
    LogicalOperator* op, LogicalOperatorType operatorType, vector<LogicalOperator*>& result) {
    if (op->getLogicalOperatorType() == operatorType) {
        result.push_back(op);
    }
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        collectOperatorsRecursive(op->getChild(i).get(), operatorType, result);
    }
}

void LogicalPlanUtil::encodeHashJoin(LogicalOperator* logicalOperator, string& encodeString) {
    auto logicalHashJoin = (LogicalHashJoin*)logicalOperator;
    encodeString += "HJ(" + logicalHashJoin->getJoinNode()->getRawName() + ")";
}

void LogicalPlanUtil::encodeExtend(LogicalOperator* logicalOperator, string& encodeString) {
    auto logicalExtend = (LogicalExtend*)logicalOperator;
    encodeString += "E(" + logicalExtend->getNbrNodeExpression()->getRawName() + ")";
}

void LogicalPlanUtil::encodeScanNodeID(LogicalOperator* logicalOperator, string& encodeString) {
    auto logicalScanNodeID = (LogicalScanNodeID*)logicalOperator;
    encodeString += "S(" + logicalScanNodeID->getNode()->getRawName() + ")";
}

} // namespace planner
} // namespace graphflow
