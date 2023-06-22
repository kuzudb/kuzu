#include "planner/join_order/cost_model.h"
#include "planner/join_order_enumerator.h"
#include "planner/logical_plan/logical_operator/logical_hash_join.h"
#include "planner/logical_plan/logical_operator/logical_intersect.h"
#include "planner/query_planner.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void JoinOrderEnumerator::appendHashJoin(const expression_vector& joinNodeIDs, JoinType joinType,
    LogicalPlan& probePlan, LogicalPlan& buildPlan) {
    auto hashJoin = make_shared<LogicalHashJoin>(
        joinNodeIDs, joinType, probePlan.getLastOperator(), buildPlan.getLastOperator());
    // Apply flattening to probe side
    auto groupsPosToFlattenOnProbeSide = hashJoin->getGroupsPosToFlattenOnProbeSide();
    queryPlanner->appendFlattens(groupsPosToFlattenOnProbeSide, probePlan);
    hashJoin->setChild(0, probePlan.getLastOperator());
    // Apply flattening to build side
    queryPlanner->appendFlattens(hashJoin->getGroupsPosToFlattenOnBuildSide(), buildPlan);
    hashJoin->setChild(1, buildPlan.getLastOperator());
    hashJoin->computeFactorizedSchema();
    // Check for sip
    auto ratio = probePlan.getCardinality() / buildPlan.getCardinality();
    if (ratio > common::PlannerKnobs::SIP_RATIO) {
        hashJoin->setSIP(SidewaysInfoPassing::PROHIBIT_PROBE_TO_BUILD);
    } else {
        hashJoin->setSIP(SidewaysInfoPassing::PROHIBIT_BUILD_TO_PROBE);
    }
    // Update cost
    probePlan.setCost(CostModel::computeHashJoinCost(joinNodeIDs, probePlan, buildPlan));
    // Update cardinality
    probePlan.setCardinality(
        queryPlanner->cardinalityEstimator->estimateHashJoin(joinNodeIDs, probePlan, buildPlan));
    probePlan.setLastOperator(std::move(hashJoin));
}

void JoinOrderEnumerator::appendMarkJoin(const expression_vector& joinNodeIDs,
    const std::shared_ptr<Expression>& mark, LogicalPlan& probePlan, LogicalPlan& buildPlan) {
    auto hashJoin = make_shared<LogicalHashJoin>(
        joinNodeIDs, mark, probePlan.getLastOperator(), buildPlan.getLastOperator());
    // Apply flattening to probe side
    queryPlanner->appendFlattens(hashJoin->getGroupsPosToFlattenOnProbeSide(), probePlan);
    hashJoin->setChild(0, probePlan.getLastOperator());
    // Apply flattening to build side
    queryPlanner->appendFlattens(hashJoin->getGroupsPosToFlattenOnBuildSide(), buildPlan);
    hashJoin->setChild(1, buildPlan.getLastOperator());
    hashJoin->computeFactorizedSchema();
    // update cost. Mark join does not change cardinality.
    probePlan.setCost(CostModel::computeMarkJoinCost(joinNodeIDs, probePlan, buildPlan));
    probePlan.setLastOperator(std::move(hashJoin));
}

void JoinOrderEnumerator::appendIntersect(const std::shared_ptr<Expression>& intersectNodeID,
    binder::expression_vector& boundNodeIDs, LogicalPlan& probePlan,
    std::vector<std::unique_ptr<LogicalPlan>>& buildPlans) {
    assert(boundNodeIDs.size() == buildPlans.size());
    std::vector<std::shared_ptr<LogicalOperator>> buildChildren;
    binder::expression_vector keyNodeIDs;
    for (auto i = 0u; i < buildPlans.size(); ++i) {
        keyNodeIDs.push_back(boundNodeIDs[i]);
        buildChildren.push_back(buildPlans[i]->getLastOperator());
    }
    auto intersect = make_shared<LogicalIntersect>(intersectNodeID, std::move(keyNodeIDs),
        probePlan.getLastOperator(), std::move(buildChildren));
    queryPlanner->appendFlattens(intersect->getGroupsPosToFlattenOnProbeSide(), probePlan);
    intersect->setChild(0, probePlan.getLastOperator());
    for (auto i = 0u; i < buildPlans.size(); ++i) {
        queryPlanner->appendFlattens(
            intersect->getGroupsPosToFlattenOnBuildSide(i), *buildPlans[i]);
        intersect->setChild(i + 1, buildPlans[i]->getLastOperator());
        auto ratio = probePlan.getCardinality() / buildPlans[i]->getCardinality();
        if (ratio > common::PlannerKnobs::SIP_RATIO) {
            intersect->setSIP(SidewaysInfoPassing::PROHIBIT_PROBE_TO_BUILD);
        }
    }
    intersect->computeFactorizedSchema();
    // update cost
    probePlan.setCost(CostModel::computeIntersectCost(probePlan, buildPlans));
    // update cardinality
    probePlan.setCardinality(
        queryPlanner->cardinalityEstimator->estimateIntersect(boundNodeIDs, probePlan, buildPlans));
    probePlan.setLastOperator(std::move(intersect));
}

} // namespace planner
} // namespace kuzu
