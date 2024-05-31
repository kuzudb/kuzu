#include "planner/operator/scan/logical_scan_node_table.h"

namespace kuzu {
namespace planner {

LogicalScanNodeTable::LogicalScanNodeTable(const LogicalScanNodeTable& other)
    : LogicalOperator{type_}, scanType{other.scanType}, nodeID{other.nodeID},
      nodeTableIDs{other.nodeTableIDs}, properties{other.properties},
      propertyPredicates{copyVector(other.propertyPredicates)} {
    if (other.extraInfo != nullptr) {
        setExtraInfo(other.extraInfo->copy());
    }
}

void LogicalScanNodeTable::computeFactorizedSchema() {
    createEmptySchema();
    const auto groupPos = schema->createGroup();
    KU_ASSERT(groupPos == 0);
    schema->insertToGroupAndScope(nodeID, groupPos);
    for (auto& property : properties) {
        schema->insertToGroupAndScope(property, groupPos);
    }
    switch (scanType) {
    case LogicalScanNodeTableType::OFFSET_SCAN: {
        schema->setGroupAsSingleState(groupPos);
        auto recursiveJoinInfo = extraInfo->constCast<RecursiveJoinScanInfo>();
        schema->insertToGroupAndScope(recursiveJoinInfo.nodePredicateExecFlag, groupPos);
    } break;
    case LogicalScanNodeTableType::PRIMARY_KEY_SCAN: {
        schema->setGroupAsSingleState(groupPos);
    } break;
    default:
        break;
    }
}

void LogicalScanNodeTable::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    schema->insertToGroupAndScope(nodeID, 0);
    for (auto& property : properties) {
        schema->insertToGroupAndScope(property, 0);
    }
    if (scanType == LogicalScanNodeTableType::OFFSET_SCAN) {
        auto recursiveJoinInfo = extraInfo->constCast<RecursiveJoinScanInfo>();
        schema->insertToGroupAndScope(recursiveJoinInfo.nodePredicateExecFlag, 0);
    }
}

std::unique_ptr<LogicalOperator> LogicalScanNodeTable::copy() {
    return std::make_unique<LogicalScanNodeTable>(*this);
}

} // namespace planner
} // namespace kuzu
