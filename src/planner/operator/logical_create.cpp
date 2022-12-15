#include "planner/logical_plan/logical_operator/logical_create.h"

namespace kuzu {
namespace planner {

void LogicalCreateNode::computeSchema() {
    copyChildSchema(0);
    for (auto& [node, _] : nodeAndPrimaryKeys) {
        auto groupPos = schema->createGroup();
        schema->setGroupAsSingleState(groupPos);
        schema->insertToGroupAndScope(node->getInternalIDProperty(), groupPos);
    }
}

} // namespace planner
} // namespace kuzu
