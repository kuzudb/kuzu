#include "optimizer/factorization_rewriter.h"

#include "planner/logical_plan/logical_operator/factorization_resolver.h"
#include "planner/logical_plan/logical_operator/logical_aggregate.h"
#include "planner/logical_plan/logical_operator/logical_extend.h"
#include "planner/logical_plan/logical_operator/logical_flatten.h"
#include "planner/logical_plan/logical_operator/logical_hash_join.h"
#include "planner/logical_plan/logical_operator/logical_intersect.h"
#include "planner/logical_plan/logical_operator/logical_limit.h"
#include "planner/logical_plan/logical_operator/logical_order_by.h"
#include "planner/logical_plan/logical_operator/logical_projection.h"
#include "planner/logical_plan/logical_operator/logical_skip.h"

using namespace kuzu::planner;

namespace kuzu {
namespace optimizer {

void FactorizationRewriter::rewrite(planner::LogicalPlan* plan) {
    visitOperator(plan->getLastOperator().get());
}

void FactorizationRewriter::visitOperator(planner::LogicalOperator* op) {
    // bottom-up traversal
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
        visitOperator(op->getChild(i).get());
    }
    switch (op->getOperatorType()) {
    case LogicalOperatorType::EXTEND: {
        visitExtend(op);
    } break;
    case LogicalOperatorType::HASH_JOIN: {
        visitHashJoin(op);
    } break;
    case LogicalOperatorType::INTERSECT: {
        visitIntersect(op);
    } break;
    case LogicalOperatorType::PROJECTION: {
        visitProjection(op);
    } break;
    case LogicalOperatorType::AGGREGATE: {
        visitAggregate(op);
    } break;
    case LogicalOperatorType::ORDER_BY: {
        visitOrderBy(op);
    } break;
    case LogicalOperatorType::SKIP: {
        visitSkip(op);
    } break;
    case LogicalOperatorType::LIMIT: {
        visitLimit(op);
    } break;
    default:
        break;
    }
    op->computeSchema();
}

void FactorizationRewriter::visitExtend(planner::LogicalOperator* op) {
    auto extend = (LogicalExtend*)op;
    if (!LogicalExtendFactorizationResolver::requireFlatBoundNode(extend)) {
        return;
    }
    auto groupPosToFlatten = LogicalExtendFactorizationResolver::getGroupPosToFlatten(extend);
    extend->setChild(0, appendFlattenIfNecessary(extend->getChild(0), groupPosToFlatten));
}

void FactorizationRewriter::visitHashJoin(planner::LogicalOperator* op) {
    auto hashJoin = (LogicalHashJoin*)op;
    auto groupsPosToFlattenOnProbeSide =
        LogicalHashJoinFactorizationResolver::getGroupsPosToFlattenOnProbeSide(hashJoin);
    hashJoin->setChild(0, appendFlattens(hashJoin->getChild(0), groupsPosToFlattenOnProbeSide));
    auto groupsPosToFlattenOnBuildSide =
        LogicalHashJoinFactorizationResolver::getGroupsPosToFlattenOnBuildSide(hashJoin);
    hashJoin->setChild(1, appendFlattens(hashJoin->getChild(1), groupsPosToFlattenOnBuildSide));
}

void FactorizationRewriter::visitIntersect(planner::LogicalOperator* op) {
    auto intersect = (LogicalIntersect*)op;
    auto groupsPosToFlattenOnProbeSide =
        LogicalIntersectFactorizationResolver::getGroupsPosToFlattenOnProbeSide(intersect);
    intersect->setChild(0, appendFlattens(intersect->getChild(0), groupsPosToFlattenOnProbeSide));
    for (auto i = 0u; i < intersect->getNumBuilds(); ++i) {
        auto groupPosToFlatten =
            LogicalIntersectFactorizationResolver::getGroupPosToFlattenOnBuildSide(intersect, i);
        auto childIdx = i + 1; // skip probe
        intersect->setChild(
            childIdx, appendFlattenIfNecessary(intersect->getChild(childIdx), groupPosToFlatten));
    }
}

void FactorizationRewriter::visitProjection(planner::LogicalOperator* op) {
    auto projection = (LogicalProjection*)op;
    for (auto& expression : projection->getExpressionsToProject()) {
        auto dependentGroupsPos = op->getChild(0)->getSchema()->getDependentGroupsPos(expression);
        auto groupsPosToFlatten = FlattenAllButOneFactorizationResolver::getGroupsPosToFlatten(
            dependentGroupsPos, op->getChild(0)->getSchema());
        projection->setChild(0, appendFlattens(projection->getChild(0), groupsPosToFlatten));
    }
}

void FactorizationRewriter::visitAggregate(planner::LogicalOperator* op) {
    auto aggregate = (LogicalAggregate*)op;
    auto groupsPosToFlattenForGroupBy =
        LogicalAggregateFactorizationSolver::getGroupsPosToFlattenForGroupBy(aggregate);
    aggregate->setChild(0, appendFlattens(aggregate->getChild(0), groupsPosToFlattenForGroupBy));
    auto groupsPosToFlattenForAggregate =
        LogicalAggregateFactorizationSolver::getGroupsPosToFlattenForAggregate(aggregate);
    aggregate->setChild(0, appendFlattens(aggregate->getChild(0), groupsPosToFlattenForAggregate));
}

void FactorizationRewriter::visitOrderBy(planner::LogicalOperator* op) {
    auto orderBy = (LogicalOrderBy*)op;
    auto groupsPosToFlatten = LogicalOrderByFactorizationSolver::getGroupsPosToFlatten(orderBy);
    orderBy->setChild(0, appendFlattens(orderBy->getChild(0), groupsPosToFlatten));
}

void FactorizationRewriter::visitSkip(planner::LogicalOperator* op) {
    auto skip = (LogicalSkip*)op;
    auto groupsPosToFlatten = LogicalSkipFactorizationSolver::getGroupsPosToFlatten(skip);
    skip->setChild(0, appendFlattens(skip->getChild(0), groupsPosToFlatten));
}

void FactorizationRewriter::visitLimit(planner::LogicalOperator* op) {
    auto limit = (LogicalLimit*)op;
    auto groupsPosToFlatten = LogicalLimitFactorizationSolver::getGroupsPosToFlatten(limit);
    limit->setChild(0, appendFlattens(limit->getChild(0), groupsPosToFlatten));
}

std::shared_ptr<planner::LogicalOperator> FactorizationRewriter::appendFlattens(
    std::shared_ptr<planner::LogicalOperator> op,
    const std::unordered_set<f_group_pos>& groupsPos) {
    auto currentChild = std::move(op);
    for (auto groupPos : groupsPos) {
        currentChild = appendFlattenIfNecessary(std::move(currentChild), groupPos);
    }
    return currentChild;
}

std::shared_ptr<planner::LogicalOperator> FactorizationRewriter::appendFlattenIfNecessary(
    std::shared_ptr<planner::LogicalOperator> op, planner::f_group_pos groupPos) {
    if (op->getSchema()->getGroup(groupPos)->isFlat()) {
        return op;
    }
    auto expression = op->getSchema()->getExpressionsInScope(groupPos)[0];
    auto flatten = std::make_shared<LogicalFlatten>(expression, std::move(op));
    flatten->computeSchema();
    return flatten;
}

} // namespace optimizer
} // namespace kuzu
