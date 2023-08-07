#include "planner/logical_plan/scan/logical_scan_node_property.h"

namespace kuzu {
namespace planner {

void LogicalScanNodeProperty::computeFactorizedSchema() {
    copyChildSchema(0);
    auto groupPos = schema->getGroupPos(node->getInternalIDPropertyName());
    for (auto& property : properties) {
        schema->insertToGroupAndScope(property, groupPos);
    }
}

void LogicalScanNodeProperty::computeFlatSchema() {
    copyChildSchema(0);
    for (auto& property : properties) {
        schema->insertToGroupAndScope(property, 0);
    }
}

} // namespace planner
} // namespace kuzu
