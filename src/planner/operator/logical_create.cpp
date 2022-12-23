#include "planner/logical_plan/logical_operator/logical_create.h"

namespace kuzu {
namespace planner {

void LogicalCreateNode::computeSchema() {
    LogicalUpdateNode::computeSchema();
    for (auto& node : nodes) {
        auto groupPos = schema->createGroup();
        schema->setGroupAsSingleState(groupPos);
        schema->insertToGroupAndScope(node->getInternalIDProperty(), groupPos);
    }
}

} // namespace planner
} // namespace kuzu
