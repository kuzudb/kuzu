#include "planner/logical_plan/logical_operator/logical_create.h"

namespace kuzu {
namespace planner {

void LogicalCreateNode::computeFactorizedSchema() {
    copyChildSchema(0);
    for (auto& node : nodes) {
        auto groupPos = schema->createGroup();
        schema->setGroupAsSingleState(groupPos);
        schema->insertToGroupAndScope(node->getInternalIDProperty(), groupPos);
    }
}

void LogicalCreateNode::computeFlatSchema() {
    copyChildSchema(0);
    for (auto& node : nodes) {
        schema->insertToGroupAndScope(node->getInternalIDProperty(), 0);
    }
}

} // namespace planner
} // namespace kuzu
