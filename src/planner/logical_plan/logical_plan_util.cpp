#include "include/logical_plan_util.h"

#include "src/planner/logical_plan/logical_operator/include/logical_extend.h"
#include "src/planner/logical_plan/logical_operator/include/logical_hash_join.h"
#include "src/planner/logical_plan/logical_operator/include/logical_intersect.h"
#include "src/planner/logical_plan/logical_operator/include/logical_scan_node_id.h"

namespace graphflow {
namespace planner {

shared_ptr<NodeExpression> LogicalPlanUtil::getSequentialNode(LogicalPlan& plan) {
    auto pipelineSource = getCurrentPipelineSourceOperator(plan);
    if (pipelineSource->getLogicalOperatorType() != LOGICAL_SCAN_NODE_ID) {
        // Pipeline source is not ScanNodeID, meaning at least one sink has happened (e.g. HashJoin)
        // and we loose any sequential guarantees.
        return nullptr;
    }
    return ((LogicalScanNodeID*)pipelineSource)->getNode();
}

vector<LogicalOperator*> LogicalPlanUtil::collectOperators(
    LogicalOperator* root, LogicalOperatorType operatorType) {
    vector<LogicalOperator*> result;
    collectOperatorsRecursive(root, operatorType, result);
    return result;
}

LogicalOperator* LogicalPlanUtil::getCurrentPipelineSourceOperator(LogicalPlan& plan) {
    auto op = plan.getLastOperator().get();
    // Operator with more than one child will be broken into different pipelines.
    while (op->getNumChildren() == 1) {
        op = op->getChild(0).get();
    }
    assert(op != nullptr);
    return op;
}

void LogicalPlanUtil::encodeJoinRecursive(LogicalOperator* logicalOperator, string& encodeString) {
    switch (logicalOperator->getLogicalOperatorType()) {
    case LogicalOperatorType::LOGICAL_INTERSECT: {
        encodeIntersect(logicalOperator, encodeString);
        for (auto i = 0u; i < logicalOperator->getNumChildren(); ++i) {
            encodeString += "{";
            encodeJoinRecursive(logicalOperator->getChild(i).get(), encodeString);
            encodeString += "}";
        }
    } break;
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

void LogicalPlanUtil::encodeIntersect(LogicalOperator* logicalOperator, string& encodeString) {
    auto logicalIntersect = (LogicalIntersect*)logicalOperator;
    encodeString += "I(" + logicalIntersect->getIntersectNode()->getRawName() + ")";
}

void LogicalPlanUtil::encodeHashJoin(LogicalOperator* logicalOperator, string& encodeString) {
    auto logicalHashJoin = (LogicalHashJoin*)logicalOperator;
    encodeString += "HJ(";
    encodeString += logicalHashJoin->getExpressionsForPrinting() + ")";
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
