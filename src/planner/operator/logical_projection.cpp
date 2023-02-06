#include "planner/logical_plan/logical_operator/logical_projection.h"

namespace kuzu {
namespace planner {

void LogicalProjection::computeSchema() {
    auto childSchema = children[0]->getSchema();
    schema = childSchema->copy();
    schema->clearExpressionsInScope();
    for (auto i = 0u; i < expressions.size(); ++i) {
        auto expression = expressions[i];
        auto outputPos = expressionsOutputPos[i];
        // E.g. RETURN MIN(a.age), MAX(a.age). We first project each expression in aggregate. So
        // a.age might be projected twice.
        if (schema->isExpressionInScope(*expression)) {
            continue;
        }
        if (childSchema->isExpressionInScope(*expression)) { // expression to reference
            schema->insertToScope(expression, outputPos);
        } else {                           // expression to evaluate
            if (outputPos == UINT32_MAX) { // project constant
                outputPos = schema->createGroup();
                schema->setGroupAsSingleState(outputPos);
            }
            schema->insertToGroupAndScope(expression, outputPos);
        }
    }
}

std::unordered_set<uint32_t> LogicalProjection::getDiscardedGroupsPos() const {
    auto groupsPosInScopeBeforeProjection = children[0]->getSchema()->getGroupsPosInScope();
    auto groupsPosInScopeAfterProjection = schema->getGroupsPosInScope();
    std::unordered_set<uint32_t> discardGroupsPos;
    for (auto i = 0u; i < schema->getNumGroups(); ++i) {
        if (groupsPosInScopeBeforeProjection.contains(i) &&
            !groupsPosInScopeAfterProjection.contains(i)) {
            discardGroupsPos.insert(i);
        }
    }
    return discardGroupsPos;
}

} // namespace planner
} // namespace kuzu
