#include "planner/join_order/cardinality_estimator.h"

#include "planner/join_order/join_order_util.h"
#include "planner/logical_plan/logical_operator/logical_extend.h"
#include "planner/logical_plan/logical_operator/logical_scan_node.h"

namespace kuzu {
namespace planner {

void CardinalityEstimator::initNodeIDDom(binder::QueryGraph* queryGraph) {
    for (auto i = 0u; i < queryGraph->getNumQueryNodes(); ++i) {
        addNodeIDDom(*queryGraph->getQueryNode(i));
    }
    for (auto i = 0u; i < queryGraph->getNumQueryRels(); ++i) {
        auto rel = queryGraph->getQueryRel(i);
        if (common::QueryRelTypeUtils::isRecursive(rel->getRelType())) {
            addNodeIDDom(*rel->getRecursiveInfo()->node);
        }
    }
}

uint64_t CardinalityEstimator::estimateScanNode(LogicalOperator* op) {
    auto scanNode = (LogicalScanNode*)op;
    return atLeastOne(getNodeIDDom(scanNode->getNode()->getInternalIDPropertyName()));
}

uint64_t CardinalityEstimator::estimateHashJoin(const binder::expression_vector& joinNodeIDs,
    const LogicalPlan& probePlan, const LogicalPlan& buildPlan) {
    auto denominator = 1u;
    for (auto& joinNodeID : joinNodeIDs) {
        denominator *= getNodeIDDom(joinNodeID->getUniqueName());
    }
    return atLeastOne(probePlan.estCardinality *
                      JoinOrderUtil::getJoinKeysFlatCardinality(joinNodeIDs, buildPlan) /
                      denominator);
}

uint64_t CardinalityEstimator::estimateCrossProduct(
    const LogicalPlan& probePlan, const LogicalPlan& buildPlan) {
    return atLeastOne(probePlan.estCardinality * buildPlan.estCardinality);
}

uint64_t CardinalityEstimator::estimateIntersect(const binder::expression_vector& joinNodeIDs,
    const LogicalPlan& probePlan, const std::vector<std::unique_ptr<LogicalPlan>>& buildPlans) {
    // Formula 1: treat intersect as a Filter on probe side.
    uint64_t estCardinality1 =
        probePlan.estCardinality * common::PlannerKnobs::NON_EQUALITY_PREDICATE_SELECTIVITY;
    // Formula 2: assume independence on join conditions.
    auto denominator = 1u;
    for (auto& joinNodeID : joinNodeIDs) {
        denominator *= getNodeIDDom(joinNodeID->getUniqueName());
    }
    auto numerator = probePlan.estCardinality;
    for (auto& buildPlan : buildPlans) {
        numerator *= buildPlan->estCardinality;
    }
    auto estCardinality2 = numerator / denominator;
    // Pick minimum between the two formulas.
    return atLeastOne(std::min<uint64_t>(estCardinality1, estCardinality2));
}

uint64_t CardinalityEstimator::estimateFlatten(
    const LogicalPlan& childPlan, f_group_pos groupPosToFlatten) {
    auto group = childPlan.getSchema()->getGroup(groupPosToFlatten);
    return atLeastOne(childPlan.estCardinality * group->cardinalityMultiplier);
}

static bool isPrimaryKey(const binder::Expression& expression) {
    if (expression.expressionType != common::ExpressionType::PROPERTY) {
        return false;
    }
    return ((binder::PropertyExpression&)expression).isPrimaryKey();
}

uint64_t CardinalityEstimator::estimateFilter(
    const LogicalPlan& childPlan, const binder::Expression& predicate) {
    if (predicate.expressionType == common::EQUALS) {
        if (isPrimaryKey(*predicate.getChild(0)) || isPrimaryKey(*predicate.getChild(1))) {
            return 1;
        } else {
            return atLeastOne(
                childPlan.estCardinality * common::PlannerKnobs::EQUALITY_PREDICATE_SELECTIVITY);
        }
    } else {
        return atLeastOne(
            childPlan.estCardinality * common::PlannerKnobs::NON_EQUALITY_PREDICATE_SELECTIVITY);
    }
}

uint64_t CardinalityEstimator::getNumNodes(const binder::NodeExpression& node) {
    auto numNodes = 0u;
    for (auto& tableID : node.getTableIDs()) {
        numNodes += nodesStatistics.getNodeStatisticsAndDeletedIDs(tableID)->getNumTuples();
    }
    return atLeastOne(numNodes);
}

uint64_t CardinalityEstimator::getNumRels(const binder::RelExpression& rel) {
    auto numRels = 0u;
    for (auto tableID : rel.getTableIDs()) {
        numRels += relsStatistics.getRelStatistics(tableID)->getNumTuples();
    }
    return atLeastOne(numRels);
}

double CardinalityEstimator::getExtensionRate(
    const binder::RelExpression& rel, const binder::NodeExpression& boundNode) {
    auto numBoundNodes = (double)getNumNodes(boundNode);
    auto numRels = (double)getNumRels(rel);
    auto oneHopExtensionRate = numRels / numBoundNodes;
    switch (rel.getRelType()) {
    case common::QueryRelType::NON_RECURSIVE: {
        return oneHopExtensionRate;
    }
    case common::QueryRelType::VARIABLE_LENGTH:
    case common::QueryRelType::SHORTEST:
    case common::QueryRelType::ALL_SHORTEST: {
        return std::min<double>(oneHopExtensionRate * rel.getUpperBound(), numRels);
    }
    default:
        throw common::NotImplementedException("getExtensionRate()");
    }
}

} // namespace planner
} // namespace kuzu
