#include "planner/join_order/cost_model.h"
#include "planner/operator/logical_hash_join.h"
#include "planner/operator/logical_intersect.h"
#include "planner/planner.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void Planner::appendHashJoin(const expression_vector& joinNodeIDs, JoinType joinType,
    LogicalPlan& probePlan, LogicalPlan& buildPlan, LogicalPlan& resultPlan) {
    appendHashJoin(joinNodeIDs, joinType, nullptr /* mark */, probePlan, buildPlan, resultPlan);
}

void Planner::appendHashJoin(const expression_vector& joinNodeIDs, JoinType joinType,
    std::shared_ptr<Expression> mark, LogicalPlan& probePlan, LogicalPlan& buildPlan,
    LogicalPlan& resultPlan) {
    std::vector<join_condition_t> joinConditions;
    for (auto& joinNodeID : joinNodeIDs) {
        joinConditions.emplace_back(joinNodeID, joinNodeID);
    }
    auto hashJoin = make_shared<LogicalHashJoin>(joinConditions, joinType, mark,
        probePlan.getLastOperator(), buildPlan.getLastOperator());
    // Apply flattening to probe side
    auto groupsPosToFlattenOnProbeSide = hashJoin->getGroupsPosToFlattenOnProbeSide();
    appendFlattens(groupsPosToFlattenOnProbeSide, probePlan);
    hashJoin->setChild(0, probePlan.getLastOperator());
    // Apply flattening to build side
    appendFlattens(hashJoin->getGroupsPosToFlattenOnBuildSide(), buildPlan);
    hashJoin->setChild(1, buildPlan.getLastOperator());
    hashJoin->computeFactorizedSchema();
    // Check for sip
    auto ratio = probePlan.getCardinality() / buildPlan.getCardinality();
    if (ratio > PlannerKnobs::SIP_RATIO) {
        hashJoin->getSIPInfoUnsafe().position = SemiMaskPosition::PROHIBIT_PROBE_TO_BUILD;
    }
    // Update cost
    resultPlan.setCost(CostModel::computeHashJoinCost(joinNodeIDs, probePlan, buildPlan));
    // Update cardinality
    resultPlan.setCardinality(
        cardinalityEstimator.estimateHashJoin(joinNodeIDs, probePlan, buildPlan));
    resultPlan.setLastOperator(std::move(hashJoin));
}

void Planner::appendAccHashJoin(const expression_vector& joinNodeIDs, JoinType joinType,
    std::shared_ptr<Expression> mark, LogicalPlan& probePlan, LogicalPlan& buildPlan,
    LogicalPlan& resultPlan) {
    KU_ASSERT(probePlan.hasUpdate());
    tryAppendAccumulate(probePlan);
    appendHashJoin(joinNodeIDs, joinType, mark, probePlan, buildPlan, resultPlan);
    auto& sipInfo = probePlan.getLastOperator()->cast<LogicalHashJoin>().getSIPInfoUnsafe();
    sipInfo.direction = SIPDirection::PROBE_TO_BUILD;
}

void Planner::appendMarkJoin(const expression_vector& joinNodeIDs,
    const std::shared_ptr<Expression>& mark, LogicalPlan& probePlan, LogicalPlan& buildPlan) {
    std::vector<join_condition_t> joinConditions;
    for (auto& joinNodeID : joinNodeIDs) {
        joinConditions.emplace_back(joinNodeID, joinNodeID);
    }
    auto hashJoin = make_shared<LogicalHashJoin>(joinConditions, JoinType::MARK, mark,
        probePlan.getLastOperator(), buildPlan.getLastOperator());
    // Apply flattening to probe side
    appendFlattens(hashJoin->getGroupsPosToFlattenOnProbeSide(), probePlan);
    hashJoin->setChild(0, probePlan.getLastOperator());
    // Apply flattening to build side
    appendFlattens(hashJoin->getGroupsPosToFlattenOnBuildSide(), buildPlan);
    hashJoin->setChild(1, buildPlan.getLastOperator());
    hashJoin->computeFactorizedSchema();
    // update cost. Mark join does not change cardinality.
    probePlan.setCost(CostModel::computeMarkJoinCost(joinNodeIDs, probePlan, buildPlan));
    probePlan.setLastOperator(std::move(hashJoin));
}

void Planner::appendIntersect(const std::shared_ptr<Expression>& intersectNodeID,
    expression_vector& boundNodeIDs, LogicalPlan& probePlan,
    std::vector<std::unique_ptr<LogicalPlan>>& buildPlans) {
    KU_ASSERT(boundNodeIDs.size() == buildPlans.size());
    std::vector<std::shared_ptr<LogicalOperator>> buildChildren;
    expression_vector keyNodeIDs;
    for (auto i = 0u; i < buildPlans.size(); ++i) {
        keyNodeIDs.push_back(boundNodeIDs[i]);
        buildChildren.push_back(buildPlans[i]->getLastOperator());
    }
    auto intersect = make_shared<LogicalIntersect>(intersectNodeID, std::move(keyNodeIDs),
        probePlan.getLastOperator(), std::move(buildChildren));
    appendFlattens(intersect->getGroupsPosToFlattenOnProbeSide(), probePlan);
    intersect->setChild(0, probePlan.getLastOperator());
    for (auto i = 0u; i < buildPlans.size(); ++i) {
        appendFlattens(intersect->getGroupsPosToFlattenOnBuildSide(i), *buildPlans[i]);
        intersect->setChild(i + 1, buildPlans[i]->getLastOperator());
        auto ratio = probePlan.getCardinality() / buildPlans[i]->getCardinality();
        if (ratio > PlannerKnobs::SIP_RATIO) {
            intersect->getSIPInfoUnsafe().position = SemiMaskPosition::PROHIBIT;
        }
    }
    intersect->computeFactorizedSchema();
    // update cost
    probePlan.setCost(CostModel::computeIntersectCost(probePlan, buildPlans));
    // update cardinality
    probePlan.setCardinality(
        cardinalityEstimator.estimateIntersect(boundNodeIDs, probePlan, buildPlans));
    probePlan.setLastOperator(std::move(intersect));
}

} // namespace planner
} // namespace kuzu
