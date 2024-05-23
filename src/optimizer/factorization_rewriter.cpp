#include "optimizer/factorization_rewriter.h"

#include "binder/expression_visitor.h"
#include "common/cast.h"
#include "planner/operator/extend/logical_extend.h"
#include "planner/operator/extend/logical_recursive_extend.h"
#include "planner/operator/factorization/flatten_resolver.h"
#include "planner/operator/logical_accumulate.h"
#include "planner/operator/logical_aggregate.h"
#include "planner/operator/logical_distinct.h"
#include "planner/operator/logical_filter.h"
#include "planner/operator/logical_flatten.h"
#include "planner/operator/logical_hash_join.h"
#include "planner/operator/logical_intersect.h"
#include "planner/operator/logical_limit.h"
#include "planner/operator/logical_mark_accmulate.h"
#include "planner/operator/logical_order_by.h"
#include "planner/operator/logical_projection.h"
#include "planner/operator/logical_union.h"
#include "planner/operator/logical_unwind.h"
#include "planner/operator/persistent/logical_copy_to.h"
#include "planner/operator/persistent/logical_delete.h"
#include "planner/operator/persistent/logical_insert.h"
#include "planner/operator/persistent/logical_merge.h"
#include "planner/operator/persistent/logical_set.h"

using namespace kuzu::common;
using namespace kuzu::binder;
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
    visitOperatorSwitch(op);
    op->computeFactorizedSchema();
}

void FactorizationRewriter::visitExtend(planner::LogicalOperator* op) {
    auto extend = (LogicalExtend*)op;
    auto groupsPosToFlatten = extend->getGroupsPosToFlatten();
    extend->setChild(0, appendFlattens(extend->getChild(0), groupsPosToFlatten));
}

void FactorizationRewriter::visitRecursiveExtend(planner::LogicalOperator* op) {
    auto extend = (LogicalRecursiveExtend*)op;
    auto groupsPosToFlatten = extend->getGroupsPosToFlatten();
    extend->setChild(0, appendFlattens(extend->getChild(0), groupsPosToFlatten));
}

void FactorizationRewriter::visitHashJoin(planner::LogicalOperator* op) {
    auto hashJoin = (LogicalHashJoin*)op;
    auto groupsPosToFlattenOnProbeSide = hashJoin->getGroupsPosToFlattenOnProbeSide();
    hashJoin->setChild(0, appendFlattens(hashJoin->getChild(0), groupsPosToFlattenOnProbeSide));
    auto groupsPosToFlattenOnBuildSide = hashJoin->getGroupsPosToFlattenOnBuildSide();
    hashJoin->setChild(1, appendFlattens(hashJoin->getChild(1), groupsPosToFlattenOnBuildSide));
}

void FactorizationRewriter::visitIntersect(planner::LogicalOperator* op) {
    auto intersect = (LogicalIntersect*)op;
    auto groupsPosToFlattenOnProbeSide = intersect->getGroupsPosToFlattenOnProbeSide();
    intersect->setChild(0, appendFlattens(intersect->getChild(0), groupsPosToFlattenOnProbeSide));
    for (auto i = 0u; i < intersect->getNumBuilds(); ++i) {
        auto groupPosToFlatten = intersect->getGroupsPosToFlattenOnBuildSide(i);
        auto childIdx = i + 1; // skip probe
        intersect->setChild(childIdx,
            appendFlattens(intersect->getChild(childIdx), groupPosToFlatten));
    }
}

void FactorizationRewriter::visitProjection(planner::LogicalOperator* op) {
    auto projection = (LogicalProjection*)op;
    bool hasRandomFunction = false;
    for (auto& expr : projection->getExpressionsToProject()) {
        if (ExpressionVisitor::isRandom(*expr)) {
            hasRandomFunction = true;
        }
    }
    if (hasRandomFunction) {
        // Fall back to tuple-at-a-time evaluation.
        auto groupsPos = op->getChild(0)->getSchema()->getGroupsPosInScope();
        auto groupsPosToFlatten = factorization::FlattenAll::getGroupsPosToFlatten(groupsPos,
            op->getChild(0)->getSchema());
        projection->setChild(0, appendFlattens(projection->getChild(0), groupsPosToFlatten));
    } else {
        for (auto& expression : projection->getExpressionsToProject()) {
            auto dependentGroupsPos =
                op->getChild(0)->getSchema()->getDependentGroupsPos(expression);
            auto groupsPosToFlatten = factorization::FlattenAllButOne::getGroupsPosToFlatten(
                dependentGroupsPos, op->getChild(0)->getSchema());
            projection->setChild(0, appendFlattens(projection->getChild(0), groupsPosToFlatten));
        }
    }
}

void FactorizationRewriter::visitAccumulate(planner::LogicalOperator* op) {
    auto accumulate = (LogicalAccumulate*)op;
    auto groupsPosToFlatten = accumulate->getGroupPositionsToFlatten();
    accumulate->setChild(0, appendFlattens(accumulate->getChild(0), groupsPosToFlatten));
}

void FactorizationRewriter::visitMarkAccumulate(planner::LogicalOperator* op) {
    auto markAccumulate = ku_dynamic_cast<LogicalOperator*, LogicalMarkAccumulate*>(op);
    auto groupsPos = markAccumulate->getGroupsPosToFlatten();
    markAccumulate->setChild(0, appendFlattens(markAccumulate->getChild(0), groupsPos));
}

void FactorizationRewriter::visitAggregate(planner::LogicalOperator* op) {
    auto aggregate = (LogicalAggregate*)op;
    auto groupsPosToFlattenForGroupBy = aggregate->getGroupsPosToFlattenForGroupBy();
    aggregate->setChild(0, appendFlattens(aggregate->getChild(0), groupsPosToFlattenForGroupBy));
    auto groupsPosToFlattenForAggregate = aggregate->getGroupsPosToFlattenForAggregate();
    aggregate->setChild(0, appendFlattens(aggregate->getChild(0), groupsPosToFlattenForAggregate));
}

void FactorizationRewriter::visitOrderBy(planner::LogicalOperator* op) {
    auto orderBy = (LogicalOrderBy*)op;
    auto groupsPosToFlatten = orderBy->getGroupsPosToFlatten();
    orderBy->setChild(0, appendFlattens(orderBy->getChild(0), groupsPosToFlatten));
}

void FactorizationRewriter::visitLimit(planner::LogicalOperator* op) {
    auto limit = (LogicalLimit*)op;
    auto groupsPosToFlatten = limit->getGroupsPosToFlatten();
    limit->setChild(0, appendFlattens(limit->getChild(0), groupsPosToFlatten));
}

void FactorizationRewriter::visitDistinct(planner::LogicalOperator* op) {
    auto distinct = (LogicalDistinct*)op;
    auto groupsPosToFlatten = distinct->getGroupsPosToFlatten();
    distinct->setChild(0, appendFlattens(distinct->getChild(0), groupsPosToFlatten));
}

void FactorizationRewriter::visitUnwind(planner::LogicalOperator* op) {
    auto unwind = (LogicalUnwind*)op;
    auto groupsPosToFlatten = unwind->getGroupsPosToFlatten();
    unwind->setChild(0, appendFlattens(unwind->getChild(0), groupsPosToFlatten));
}

void FactorizationRewriter::visitUnion(planner::LogicalOperator* op) {
    auto union_ = (LogicalUnion*)op;
    for (auto i = 0u; i < union_->getNumChildren(); ++i) {
        auto groupsPosToFlatten = union_->getGroupsPosToFlatten(i);
        union_->setChild(i, appendFlattens(union_->getChild(i), groupsPosToFlatten));
    }
}

void FactorizationRewriter::visitFilter(planner::LogicalOperator* op) {
    auto filter = (LogicalFilter*)op;
    auto groupsPosToFlatten = filter->getGroupsPosToFlatten();
    filter->setChild(0, appendFlattens(filter->getChild(0), groupsPosToFlatten));
}

void FactorizationRewriter::visitSetProperty(planner::LogicalOperator* op) {
    auto set = op->ptrCast<LogicalSetProperty>();
    for (auto i = 0u; i < set->getInfos().size(); ++i) {
        auto groupsPos = set->getGroupsPosToFlatten(i);
        set->setChild(0, appendFlattens(set->getChild(0), groupsPos));
    }
}

void FactorizationRewriter::visitDelete(planner::LogicalOperator* op) {
    auto deleteNode = op->ptrCast<LogicalDelete>();
    auto groupsPosToFlatten = deleteNode->getGroupsPosToFlatten();
    deleteNode->setChild(0, appendFlattens(deleteNode->getChild(0), groupsPosToFlatten));
}

void FactorizationRewriter::visitInsert(planner::LogicalOperator* op) {
    auto insertNode = (LogicalInsert*)op;
    auto groupsPosToFlatten = insertNode->getGroupsPosToFlatten();
    insertNode->setChild(0, appendFlattens(insertNode->getChild(0), groupsPosToFlatten));
}

void FactorizationRewriter::visitMerge(planner::LogicalOperator* op) {
    auto merge = (LogicalMerge*)op;
    auto groupsPosToFlatten = merge->getGroupsPosToFlatten();
    merge->setChild(0, appendFlattens(merge->getChild(0), groupsPosToFlatten));
}

void FactorizationRewriter::visitCopyTo(planner::LogicalOperator* op) {
    auto copyTo = (LogicalCopyTo*)op;
    auto groupsPosToFlatten = copyTo->getGroupsPosToFlatten();
    copyTo->setChild(0, appendFlattens(copyTo->getChild(0), groupsPosToFlatten));
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
    auto flatten = std::make_shared<LogicalFlatten>(groupPos, std::move(op));
    flatten->computeFactorizedSchema();
    return flatten;
}

} // namespace optimizer
} // namespace kuzu
