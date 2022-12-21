#include "planner/logical_plan/logical_operator/logical_scan_node_property.h"

namespace kuzu {
namespace planner {

void LogicalScanNodeProperty::computeSchema() {
    copyChildSchema(0);
    auto groupPos = schema->getGroupPos(node->getInternalIDPropertyName());
    for (auto& property : properties) {
        schema->insertToGroupAndScope(property, groupPos);
    }
}

} // namespace planner
} // namespace kuzu
