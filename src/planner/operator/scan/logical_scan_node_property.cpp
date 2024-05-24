#include "planner/operator/scan/logical_scan_node_table.h"

namespace kuzu {
namespace planner {

void LogicalScanNodeTable::computeFactorizedSchema() {
    createEmptySchema();
    const auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(nodeID, groupPos);
    for (auto& property : properties) {
        schema->insertToGroupAndScope(property, groupPos);
    }
}

void LogicalScanNodeTable::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    schema->insertToGroupAndScope(nodeID, 0);
    for (auto& property : properties) {
        schema->insertToGroupAndScope(property, 0);
    }
}

} // namespace planner
} // namespace kuzu
