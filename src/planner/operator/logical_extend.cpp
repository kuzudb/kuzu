#include "planner/logical_plan/logical_operator/logical_extend.h"

namespace kuzu {
namespace planner {

void LogicalExtend::computeSchema() {
    copyChildSchema(0);
    auto boundGroupPos = schema->getGroupPos(boundNode->getInternalIDPropertyName());
    uint32_t nbrGroupPos = 0u;
    if (!extendToNewGroup) {
        nbrGroupPos = boundGroupPos;
    } else {
        assert(schema->getGroup(boundGroupPos)->isFlat());
        nbrGroupPos = schema->createGroup();
    }
    schema->insertToGroupAndScope(nbrNode->getInternalIDProperty(), nbrGroupPos);
    for (auto& property : properties) {
        schema->insertToGroupAndScope(property, nbrGroupPos);
    }
}

} // namespace planner
} // namespace kuzu
