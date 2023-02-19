#include "planner/logical_plan/logical_operator/logical_union.h"

#include "planner/logical_plan/logical_operator/factorization_resolver.h"
#include "planner/logical_plan/logical_operator/sink_util.h"

namespace kuzu {
namespace planner {

void LogicalUnion::computeSchema() {
    auto firstChildSchema = children[0]->getSchema();
    schema = std::make_unique<Schema>();
    SinkOperatorUtil::recomputeSchema(
        *firstChildSchema, firstChildSchema->getExpressionsInScope(), *schema);
}

std::unique_ptr<LogicalOperator> LogicalUnion::copy() {
    std::vector<std::shared_ptr<LogicalOperator>> copiedChildren;
    for (auto i = 0u; i < getNumChildren(); ++i) {
        copiedChildren.push_back(getChild(i)->copy());
    }
    return make_unique<LogicalUnion>(expressionsToUnion, std::move(copiedChildren));
}

std::unordered_set<f_group_pos> LogicalUnionFactorizationSolver::getGroupsPosToFlatten(
    uint32_t numExpressions, kuzu::planner::LogicalOperator* unionChild,
    const std::vector<std::shared_ptr<LogicalOperator>>& unionChildren) {
    std::unordered_set<f_group_pos> groupsPos;
    for (auto i = 0u; i < numExpressions; ++i) {
        if (requireFlatExpression(i, unionChildren)) {
            auto expression = unionChild->getSchema()->getExpressionsInScope()[i];
            groupsPos.insert(unionChild->getSchema()->getGroupPos(*expression));
        }
    }
    return FlattenAllFactorizationSolver::getGroupsPosToFlatten(groupsPos, unionChild->getSchema());
}

bool LogicalUnionFactorizationSolver::requireFlatExpression(
    uint32_t expressionIdx, const std::vector<std::shared_ptr<LogicalOperator>>& unionChildren) {
    for (auto& child : unionChildren) {
        auto expression = child->getSchema()->getExpressionsInScope()[expressionIdx];
        if (child->getSchema()->getGroup(expression)->isFlat()) {
            return true;
        }
    }
    return false;
}

} // namespace planner
} // namespace kuzu
