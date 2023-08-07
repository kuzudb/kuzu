#include "planner/logical_plan/scan/logical_scan_node.h"

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

void LogicalIndexScanNode::computeFactorizedSchema() {
    copyChildSchema(0);
    auto groupPos = 0u;
    if (schema->isExpressionInScope(*indexExpression)) {
        groupPos = schema->getGroupPos(*indexExpression);
    }
    schema->insertToGroupAndScope(node->getInternalIDProperty(), groupPos);
}

void LogicalIndexScanNode::computeFlatSchema() {
    copyChildSchema(0);
    schema->insertToGroupAndScope(node->getInternalIDProperty(), 0);
}

} // namespace planner
} // namespace kuzu
