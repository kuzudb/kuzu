#include "optimizer/factorization_rewriter.h"

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
#include "planner/operator/logical_order_by.h"
#include "planner/operator/logical_projection.h"
#include "planner/operator/logical_union.h"
#include "planner/operator/logical_unwind.h"
#include "planner/operator/persistent/logical_copy_to.h"
#include "planner/operator/persistent/logical_delete.h"
#include "planner/operator/persistent/logical_insert.h"
#include "planner/operator/persistent/logical_merge.h"
#include "planner/operator/persistent/logical_set.h"

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
        intersect->setChild(
            childIdx, appendFlattens(intersect->getChild(childIdx), groupPosToFlatten));
    }
}

void FactorizationRewriter::visitProjection(planner::LogicalOperator* op) {
    auto projection = (LogicalProjection*)op;
    for (auto& expression : projection->getExpressionsToProject()) {
        auto dependentGroupsPos = op->getChild(0)->getSchema()->getDependentGroupsPos(expression);
        auto groupsPosToFlatten = factorization::FlattenAllButOne::getGroupsPosToFlatten(
            dependentGroupsPos, op->getChild(0)->getSchema());
        projection->setChild(0, appendFlattens(projection->getChild(0), groupsPosToFlatten));
    }
}

void FactorizationRewriter::visitAccumulate(planner::LogicalOperator* op) {
    auto accumulate = (LogicalAccumulate*)op;
    auto groupsPosToFlatten = accumulate->getGroupPositionsToFlatten();
    accumulate->setChild(0, appendFlattens(accumulate->getChild(0), groupsPosToFlatten));
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

void FactorizationRewriter::visitSetNodeProperty(planner::LogicalOperator* op) {
    auto setNodeProperty = (LogicalSetNodeProperty*)op;
    for (auto& info : setNodeProperty->getInfosRef()) {
        auto node = reinterpret_cast<NodeExpression*>(info->nodeOrRel.get());
        auto lhsNodeID = node->getInternalID();
        auto rhs = info->setItem.second;
        // flatten rhs
        auto rhsDependentGroupsPos = op->getChild(0)->getSchema()->getDependentGroupsPos(rhs);
        auto rhsGroupsPosToFlatten = factorization::FlattenAllButOne::getGroupsPosToFlatten(
            rhsDependentGroupsPos, op->getChild(0)->getSchema());
        setNodeProperty->setChild(
            0, appendFlattens(setNodeProperty->getChild(0), rhsGroupsPosToFlatten));
        // flatten lhs if needed
        auto lhsGroupPos = op->getChild(0)->getSchema()->getGroupPos(*lhsNodeID);
        auto rhsLeadingGroupPos =
            SchemaUtils::getLeadingGroupPos(rhsDependentGroupsPos, *op->getChild(0)->getSchema());
        if (lhsGroupPos != rhsLeadingGroupPos) {
            setNodeProperty->setChild(
                0, appendFlattenIfNecessary(setNodeProperty->getChild(0), lhsGroupPos));
        }
    }
}

void FactorizationRewriter::visitSetRelProperty(planner::LogicalOperator* op) {
    auto setRelProperty = (LogicalSetRelProperty*)op;
    for (auto i = 0u; i < setRelProperty->getInfosRef().size(); ++i) {
        auto groupsPosToFlatten = setRelProperty->getGroupsPosToFlatten(i);
        setRelProperty->setChild(
            0, appendFlattens(setRelProperty->getChild(0), groupsPosToFlatten));
    }
}

void FactorizationRewriter::visitDeleteNode(planner::LogicalOperator* op) {
    auto deleteNode = (LogicalDeleteNode*)op;
    auto groupsPosToFlatten = deleteNode->getGroupsPosToFlatten();
    deleteNode->setChild(0, appendFlattens(deleteNode->getChild(0), groupsPosToFlatten));
}

void FactorizationRewriter::visitDeleteRel(planner::LogicalOperator* op) {
    auto deleteRel = (LogicalDeleteRel*)op;
    for (auto i = 0u; i < deleteRel->getRelsRef().size(); ++i) {
        auto groupsPosToFlatten = deleteRel->getGroupsPosToFlatten(i);
        deleteRel->setChild(0, appendFlattens(deleteRel->getChild(0), groupsPosToFlatten));
    }
}

void FactorizationRewriter::visitInsertNode(planner::LogicalOperator* op) {
    auto insertNode = (LogicalInsertNode*)op;
    auto groupsPosToFlatten = insertNode->getGroupsPosToFlatten();
    insertNode->setChild(0, appendFlattens(insertNode->getChild(0), groupsPosToFlatten));
}

void FactorizationRewriter::visitInsertRel(planner::LogicalOperator* op) {
    auto insertRel = (LogicalInsertRel*)op;
    auto groupsPosToFlatten = insertRel->getGroupsPosToFlatten();
    insertRel->setChild(0, appendFlattens(insertRel->getChild(0), groupsPosToFlatten));
}

void FactorizationRewriter::visitMerge(planner::LogicalOperator* op) {
    auto merge = (LogicalMerge*)op;
    auto groupsPosToFlatten = merge->getGroupsPosToFlatten();
    merge->setChild(0, appendFlattens(merge->getChild(0), groupsPosToFlatten));
}

void FactorizationRewriter::visitCopyTo(planner::LogicalOperator* op) {
    auto copyTo = (LogicalCopyTo*)op;
    if (copyTo->getConfig()->fileType == common::FileType::CSV) {
        auto groupsPosToFlatten = copyTo->getGroupsPosToFlatten();
        copyTo->setChild(0, appendFlattens(copyTo->getChild(0), groupsPosToFlatten));
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
    auto flatten = std::make_shared<LogicalFlatten>(groupPos, std::move(op));
    flatten->computeFactorizedSchema();
    return flatten;
}

} // namespace optimizer
} // namespace kuzu
