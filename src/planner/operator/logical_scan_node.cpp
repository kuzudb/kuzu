#include "planner/logical_plan/logical_operator/logical_scan_node.h"

namespace kuzu {
namespace planner {

void LogicalScanNode::computeSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(node->getInternalIDProperty(), groupPos);
}

void LogicalIndexScanNode::computeSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    schema->setGroupAsSingleState(groupPos);
    schema->insertToGroupAndScope(node->getInternalIDProperty(), groupPos);
}

} // namespace planner
} // namespace kuzu
