#include "planner/operator/scan/logical_scan_node.h"

namespace kuzu {
namespace planner {

void LogicalScanNode::computeFactorizedSchema() {
    createEmptySchema();
    auto groupPos = schema->createGroup();
    schema->insertToGroupAndScope(node->getInternalIDProperty(), groupPos);
}

void LogicalScanNode::computeFlatSchema() {
    createEmptySchema();
    schema->createGroup();
    schema->insertToGroupAndScope(node->getInternalIDProperty(), 0);
}

} // namespace planner
} // namespace kuzu
