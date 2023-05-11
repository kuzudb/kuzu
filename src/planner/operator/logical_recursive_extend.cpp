#include "planner/logical_plan/logical_operator/logical_recursive_extend.h"

#include "planner/logical_plan/logical_operator/sink_util.h"

namespace kuzu {
namespace planner {

f_group_pos_set LogicalRecursiveExtend::getGroupsPosToFlatten() {
    f_group_pos_set result;
    auto inSchema = children[0]->getSchema();
    auto boundNodeGroupPos = inSchema->getGroupPos(*boundNode->getInternalIDProperty());
    if (!inSchema->getGroup(boundNodeGroupPos)->isFlat()) {
        result.insert(boundNodeGroupPos);
    }
    return result;
}

void LogicalRecursiveExtend::computeFlatSchema() {
    copyChildSchema(0);
    schema->insertToGroupAndScope(nbrNode->getInternalIDProperty(), 0);
    schema->insertToGroupAndScope(rel, 0);
}

void LogicalRecursiveExtend::computeFactorizedSchema() {
    createEmptySchema();
    auto childSchema = children[0]->getSchema();
    SinkOperatorUtil::recomputeSchema(*childSchema, childSchema->getExpressionsInScope(), *schema);
    auto nbrGroupPos = schema->createGroup();
    schema->insertToGroupAndScope(nbrNode->getInternalIDProperty(), nbrGroupPos);
    schema->insertToGroupAndScope(rel, nbrGroupPos);
}

} // namespace planner
} // namespace kuzu
