#include "optimizer/factorization_rewriter.h"

#include "planner/logical_plan/logical_operator/factorization_resolver.h"
#include "planner/logical_plan/logical_operator/logical_aggregate.h"
#include "planner/logical_plan/logical_operator/logical_delete.h"
#include "planner/logical_plan/logical_operator/logical_distinct.h"
#include "planner/logical_plan/logical_operator/logical_extend.h"
#include "planner/logical_plan/logical_operator/logical_filter.h"
#include "planner/logical_plan/logical_operator/logical_flatten.h"
#include "planner/logical_plan/logical_operator/logical_hash_join.h"
#include "planner/logical_plan/logical_operator/logical_intersect.h"
#include "planner/logical_plan/logical_operator/logical_limit.h"
#include "planner/logical_plan/logical_operator/logical_order_by.h"
#include "planner/logical_plan/logical_operator/logical_projection.h"
#include "planner/logical_plan/logical_operator/logical_set.h"
#include "planner/logical_plan/logical_operator/logical_skip.h"
#include "planner/logical_plan/logical_operator/logical_union.h"
#include "planner/logical_plan/logical_operator/logical_unwind.h"

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
    case LogicalOperatorType::DISTINCT: {
        visitDistinct(op);
    } break;
    case LogicalOperatorType::UNWIND: {
        visitUnwind(op);
    } break;
    case LogicalOperatorType::UNION_ALL: {
        visitUnion(op);
    } break;
    case LogicalOperatorType::FILTER: {
        visitFilter(op);
    } break;
    case LogicalOperatorType::SET_NODE_PROPERTY: {
        visitSetNodeProperty(op);
    } break;
    case LogicalOperatorType::SET_REL_PROPERTY: {
        visitSetRelProperty(op);
    } break;
    case LogicalOperatorType::DELETE_REL: {
        visitDeleteRel(op);
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

void FactorizationRewriter::visitDistinct(planner::LogicalOperator* op) {
    auto distinct = (LogicalDistinct*)op;
    auto groupsPosToFlatten = LogicalDistinctFactorizationSolver::getGroupsPosToFlatten(distinct);
    distinct->setChild(0, appendFlattens(distinct->getChild(0), groupsPosToFlatten));
}

void FactorizationRewriter::visitUnwind(planner::LogicalOperator* op) {
    auto unwind = (LogicalUnwind*)op;
    auto groupsPosToFlatten = LogicalUnwindFactorizationSolver::getGroupsPosToFlatten(unwind);
    unwind->setChild(0, appendFlattens(unwind->getChild(0), groupsPosToFlatten));
}

void FactorizationRewriter::visitUnion(planner::LogicalOperator* op) {
    auto union_ = (LogicalUnion*)op;
    auto numExpressions = union_->getExpressionsToUnion().size();
    for (auto i = 0u; i < union_->getNumChildren(); ++i) {
        auto groupsPosToFlatten = LogicalUnionFactorizationSolver::getGroupsPosToFlatten(
            numExpressions, union_->getChild(i).get(), union_->getChildren());
        union_->setChild(i, appendFlattens(union_->getChild(i), groupsPosToFlatten));
    }
}

void FactorizationRewriter::visitFilter(planner::LogicalOperator* op) {
    auto filter = (LogicalFilter*)op;
    auto groupsPosToFlatten = LogicalFilterFactorizationSolver::getGroupsPosToFlatten(filter);
    filter->setChild(0, appendFlattens(filter->getChild(0), groupsPosToFlatten));
}

void FactorizationRewriter::visitSetNodeProperty(planner::LogicalOperator* op) {
    auto setNodeProperty = (LogicalSetNodeProperty*)op;
    for (auto i = 0u; i < setNodeProperty->getNumNodes(); ++i) {
        auto lhsNodeID = setNodeProperty->getNode(i)->getInternalIDProperty();
        auto rhs = setNodeProperty->getSetItem(i).second;
        auto groupsPosToFlatten =
            LogicalSetNodePropertyFactorizationSolver::getGroupsPosToFlattenForRhs(
                rhs, op->getChild(0).get());
        setNodeProperty->setChild(
            0, appendFlattens(setNodeProperty->getChild(0), groupsPosToFlatten));
        if (LogicalSetNodePropertyFactorizationSolver::requireFlatLhs(
                lhsNodeID, rhs, op->getChild(0).get())) {
            auto groupPosToFlatten = op->getChild(0)->getSchema()->getGroupPos(*lhsNodeID);
            setNodeProperty->setChild(
                0, appendFlattenIfNecessary(setNodeProperty->getChild(0), groupPosToFlatten));
        }
    }
}

void FactorizationRewriter::visitSetRelProperty(planner::LogicalOperator* op) {
    auto setRelProperty = (LogicalSetRelProperty*)op;
    for (auto i = 0u; i < setRelProperty->getNumRels(); ++i) {
        auto groupsPosToFlatten = LogicalSetRelPropertyFactorizationSolver::getGroupsPosToFlatten(
            setRelProperty->getRel(i), setRelProperty->getSetItem(i).second, op->getChild(0).get());
        setRelProperty->setChild(
            0, appendFlattens(setRelProperty->getChild(0), groupsPosToFlatten));
    }
}

void FactorizationRewriter::visitDeleteRel(planner::LogicalOperator* op) {
    auto deleteRel = (LogicalDeleteRel*)op;
    for (auto i = 0u; i < deleteRel->getNumRels(); ++i) {
        auto groupsPosToFlatten = LogicalDeleteRelFactorizationSolver::getGroupsPosToFlatten(
            deleteRel->getRel(i), deleteRel->getChild(0).get());
        deleteRel->setChild(0, appendFlattens(deleteRel->getChild(0), groupsPosToFlatten));
    }
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
