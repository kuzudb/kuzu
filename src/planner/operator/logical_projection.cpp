#include "planner/logical_plan/logical_operator/logical_projection.h"

namespace kuzu {
namespace planner {

void LogicalProjection::computeFactorizedSchema() {
    auto childSchema = children[0]->getSchema();
    schema = childSchema->copy();
    schema->clearExpressionsInScope();
    for (auto& expression : expressions) {
        // E.g. RETURN MIN(a.age), MAX(a.age). We first project each expression in aggregate. So
        // a.age might be projected twice.
        if (schema->isExpressionInScope(*expression)) {
            continue;
        }
        auto groupPos = INVALID_F_GROUP_POS;
        if (childSchema->isExpressionInScope(*expression)) { // expression to reference
            groupPos = childSchema->getGroupPos(*expression);
            schema->insertToScope(expression, groupPos);
        } else { // expression to evaluate
            auto dependentGroupPos = childSchema->getDependentGroupsPos(expression);
            SchemaUtils::validateAtMostOneUnFlatGroup(dependentGroupPos, *childSchema);
            if (dependentGroupPos.empty()) { // constant
                groupPos = schema->createGroup();
                schema->setGroupAsSingleState(groupPos);
            } else {
                groupPos = SchemaUtils::getLeadingGroupPos(dependentGroupPos, *childSchema);
            }
            schema->insertToGroupAndScope(expression, groupPos);
        }
    }
}

void LogicalProjection::computeFlatSchema() {
    copyChildSchema(0);
    auto childSchema = children[0]->getSchema();
    schema->clearExpressionsInScope();
    for (auto& expression : expressions) {
        if (schema->isExpressionInScope(*expression)) {
            continue;
        }
        if (childSchema->isExpressionInScope(*expression)) {
            schema->insertToScope(expression, 0);
        } else {
            schema->insertToGroupAndScope(expression, 0);
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
