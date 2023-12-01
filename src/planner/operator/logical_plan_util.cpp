#include "planner/operator/logical_plan_util.h"

#include "planner/operator/extend/logical_extend.h"
#include "planner/operator/extend/logical_recursive_extend.h"
#include "planner/operator/logical_hash_join.h"
#include "planner/operator/logical_intersect.h"
#include "planner/operator/scan/logical_scan_internal_id.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void LogicalPlanUtil::encodeJoinRecursive(
    LogicalOperator* logicalOperator, std::string& encodeString) {
    switch (logicalOperator->getOperatorType()) {
    case LogicalOperatorType::CROSS_PRODUCT: {
        encodeCrossProduct(logicalOperator, encodeString);
        for (auto i = 0u; i < logicalOperator->getNumChildren(); ++i) {
            encodeString += "{";
            encodeJoinRecursive(logicalOperator->getChild(i).get(), encodeString);
            encodeString += "}";
        }
    } break;
    case LogicalOperatorType::INTERSECT: {
        encodeIntersect(logicalOperator, encodeString);
        for (auto i = 0u; i < logicalOperator->getNumChildren(); ++i) {
            encodeString += "{";
            encodeJoinRecursive(logicalOperator->getChild(i).get(), encodeString);
            encodeString += "}";
        }
    } break;
    case LogicalOperatorType::HASH_JOIN: {
        encodeHashJoin(logicalOperator, encodeString);
        encodeString += "{";
        encodeJoinRecursive(logicalOperator->getChild(0).get(), encodeString);
        encodeString += "}{";
        encodeJoinRecursive(logicalOperator->getChild(1).get(), encodeString);
        encodeString += "}";
    } break;
    case LogicalOperatorType::EXTEND: {
        encodeExtend(logicalOperator, encodeString);
        encodeJoinRecursive(logicalOperator->getChild(0).get(), encodeString);
    } break;
    case LogicalOperatorType::RECURSIVE_EXTEND: {
        encodeRecursiveExtend(logicalOperator, encodeString);
        encodeJoinRecursive(logicalOperator->getChild(0).get(), encodeString);
    } break;
    case LogicalOperatorType::SCAN_INTERNAL_ID: {
        encodeScanInternalID(logicalOperator, encodeString);
    } break;
    default:
        for (auto i = 0u; i < logicalOperator->getNumChildren(); ++i) {
            encodeJoinRecursive(logicalOperator->getChild(i).get(), encodeString);
        }
    }
}

void LogicalPlanUtil::encodeCrossProduct(
    LogicalOperator* /*logicalOperator*/, std::string& encodeString) {
    encodeString += "CP()";
}

void LogicalPlanUtil::encodeIntersect(LogicalOperator* logicalOperator, std::string& encodeString) {
    auto logicalIntersect = reinterpret_cast<LogicalIntersect*>(logicalOperator);
    encodeString += "I(" + logicalIntersect->getIntersectNodeID()->toString() + ")";
}

void LogicalPlanUtil::encodeHashJoin(LogicalOperator* logicalOperator, std::string& encodeString) {
    auto logicalHashJoin = reinterpret_cast<LogicalHashJoin*>(logicalOperator);
    encodeString += "HJ(" + logicalHashJoin->getExpressionsForPrinting() + ")";
}

void LogicalPlanUtil::encodeExtend(LogicalOperator* logicalOperator, std::string& encodeString) {
    auto logicalExtend = reinterpret_cast<LogicalExtend*>(logicalOperator);
    encodeString += "E(" + logicalExtend->getNbrNode()->toString() + ")";
}

void LogicalPlanUtil::encodeRecursiveExtend(
    LogicalOperator* logicalOperator, std::string& encodeString) {
    auto logicalExtend = reinterpret_cast<LogicalRecursiveExtend*>(logicalOperator);
    encodeString += "RE(" + logicalExtend->getNbrNode()->toString() + ")";
}

void LogicalPlanUtil::encodeScanInternalID(
    LogicalOperator* logicalOperator, std::string& encodeString) {
    auto scan = reinterpret_cast<LogicalScanInternalID*>(logicalOperator);
    encodeString += "S(" + scan->getInternalID()->toString() + ")";
}

} // namespace planner
} // namespace kuzu
