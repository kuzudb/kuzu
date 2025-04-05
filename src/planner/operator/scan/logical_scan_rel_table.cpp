#include "planner/operator/scan/logical_scan_rel_table.h"

namespace kuzu {
namespace planner {

LogicalScanRelTable::LogicalScanRelTable(const LogicalScanRelTable& other)
    : LogicalOperator{type_}, nodeID{other.nodeID}, nodeTableIDs{other.nodeTableIDs},
      properties{other.properties}, propertyPredicates{copyVector(other.propertyPredicates)} {
    if (other.extraInfo != nullptr) {
        setExtraInfo(other.extraInfo->copy());
    }
    this->cardinality = other.cardinality;
}

void LogicalScanRelTable::computeFactorizedSchema() {
    createEmptySchema();
    const auto groupPos = schema->createGroup();
    KU_ASSERT(groupPos == 0);
    schema->insertToGroupAndScope(nodeID, groupPos);
    for (auto& property : properties) {
        schema->insertToGroupAndScope(property, groupPos);
    }
}

void LogicalScanRelTable::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    schema->insertToGroupAndScope(nodeID, 0);
    for (auto& property : properties) {
        schema->insertToGroupAndScope(property, 0);
    }
}

std::unique_ptr<LogicalOperator> LogicalScanRelTable::copy() {
    return std::make_unique<LogicalScanRelTable>(*this);
}

} // namespace planner
} // namespace kuzu
