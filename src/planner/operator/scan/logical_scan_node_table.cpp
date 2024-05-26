#include "planner/operator/scan/logical_scan_node_table.h"

namespace kuzu {
namespace planner {

LogicalScanNodeTable::LogicalScanNodeTable(const LogicalScanNodeTable& other)
    : LogicalOperator{type_}, scanType{other.scanType}, nodeID{other.nodeID},
      nodeTableIDs{other.nodeTableIDs}, properties{other.properties} {
    if (other.hasRecursiveJoinScanInfo()) {
        setRecursiveJoinScanInfo(other.recursiveJoinScanInfo->copy());
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
    if (scanType == LogicalScanNodeTableType::OFFSET_LOOK_UP) {
        schema->setGroupAsSingleState(groupPos);
    }
    if (hasRecursiveJoinScanInfo()) {
        schema->insertToGroupAndScope(recursiveJoinScanInfo->nodePredicateExecFlag, groupPos);
    }
}

void LogicalScanNodeTable::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    schema->insertToGroupAndScope(nodeID, 0);
    for (auto& property : properties) {
        schema->insertToGroupAndScope(property, 0);
    }
    if (hasRecursiveJoinScanInfo()) {
        schema->insertToGroupAndScope(recursiveJoinScanInfo->nodePredicateExecFlag, 0);
    }
}

std::unique_ptr<LogicalOperator> LogicalScanNodeTable::copy() {
    return std::make_unique<LogicalScanNodeTable>(*this);
}

} // namespace planner
} // namespace kuzu
