#include "planner/join_order/join_plan_solver.h"

#include "common/enums/extend_direction.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

LogicalPlan JoinPlanSolver::solve(const JoinTree& joinTree) {
    return solveTreeNode(*joinTree.root, nullptr);
}

LogicalPlan JoinPlanSolver::solveTreeNode(const JoinTreeNode& current, const JoinTreeNode* parent) {
    switch (current.type) {
    case JoinNodeType::EXPRESSION_SCAN: {
        return solveExprScanTreeNode(current);
    }
    case JoinNodeType::NODE_SCAN: {
        return solveNodeScanTreeNode(current);
    }
    case JoinNodeType::REL_SCAN: {
        KU_ASSERT(parent != nullptr);
        return solveRelScanTreeNode(current, *parent);
    }
    case JoinNodeType::BINARY_JOIN: {
        return solveBinaryJoinTreeNode(current);
    }
    case JoinNodeType::MULTIWAY_JOIN: {
        return solveMultiwayJoinTreeNode(current);
    }
    default:
        KU_UNREACHABLE;
    }
}

LogicalPlan JoinPlanSolver::solveExprScanTreeNode(const JoinTreeNode& treeNode) {
    auto& extraInfo = treeNode.extraInfo->constCast<ExtraExprScanTreeNodeInfo>();
    auto plan = LogicalPlan();
    planner->appendExpressionsScan(extraInfo.corrExprs, plan);
    planner->appendFilters(extraInfo.predicates, plan);
    planner->appendDistinct(extraInfo.corrExprs, plan);
    return plan;
}

static ExtendDirection getExtendDirection(const RelExpression& rel,
    const NodeExpression& boundNode) {
    if (rel.getDirectionType() == binder::RelDirectionType::BOTH) {
        return ExtendDirection::BOTH;
    }
    if (*rel.getSrcNode() == boundNode) {
        return ExtendDirection::FWD;
    } else {
        return ExtendDirection::BWD;
    }
}

static std::shared_ptr<binder::NodeExpression> getNbrNode(const RelExpression& rel,
    const NodeExpression& boundNode) {
    if (*rel.getSrcNode() == boundNode) {
        return rel.getDstNode();
    }
    return rel.getSrcNode();
}

LogicalPlan JoinPlanSolver::solveNodeScanTreeNode(const JoinTreeNode& treeNode) {
    auto& extraInfo = treeNode.extraInfo->constCast<ExtraScanTreeNodeInfo>();
    KU_ASSERT(extraInfo.nodeInfo != nullptr);
    auto& nodeInfo = *extraInfo.nodeInfo;
    auto boundNode = nodeInfo.node;
    auto plan = LogicalPlan();
    planner->appendScanNodeTable(boundNode->getInternalID(), boundNode->getTableIDs(),
        nodeInfo.properties, plan);
    planner->appendFilters(nodeInfo.predicates, plan);
    for (auto& relInfo : extraInfo.relInfos) {
        auto rel = relInfo.rel;
        auto nbrNode = getNbrNode(*rel, *boundNode);
        auto direction = getExtendDirection(*rel, *boundNode);
        planner->appendExtend(boundNode, nbrNode, rel, direction, relInfo.properties, plan);
        planner->appendFilters(relInfo.predicates, plan);
    }
    planner->appendFilters(extraInfo.predicates, plan);
    return plan;
}

LogicalPlan JoinPlanSolver::solveRelScanTreeNode(const JoinTreeNode& treeNode,
    const JoinTreeNode& parent) {
    std::shared_ptr<NodeExpression> boundNode = nullptr;
    switch (parent.type) {
    case JoinNodeType::BINARY_JOIN:
    case JoinNodeType::MULTIWAY_JOIN: {
        auto& extraInfo = parent.extraInfo->constCast<ExtraJoinTreeNodeInfo>();
        if (extraInfo.joinNodes.size() == 1) {
            boundNode = extraInfo.joinNodes[0];
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
    auto& extraInfo = treeNode.extraInfo->constCast<ExtraScanTreeNodeInfo>();
    KU_ASSERT(extraInfo.isSingleRel());
    auto& relInfo = extraInfo.relInfos[0];
    auto rel = relInfo.rel;
    if (boundNode == nullptr) {
        boundNode = rel->getSrcNode();
    }
    auto nbrNode = getNbrNode(*rel, *boundNode);
    auto direction = getExtendDirection(*rel, *boundNode);
    auto plan = LogicalPlan();
    planner->appendScanNodeTable(boundNode->getInternalID(), boundNode->getTableIDs(),
        expression_vector{}, plan);
    planner->appendExtend(boundNode, nbrNode, rel, direction, relInfo.properties, plan);
    planner->appendFilters(relInfo.predicates, plan);
    return plan;
}

LogicalPlan JoinPlanSolver::solveBinaryJoinTreeNode(const JoinTreeNode& treeNode) {
    auto probePlan = solveTreeNode(*treeNode.children[0], &treeNode);
    auto p = probePlan.toString();
    auto buildPlan = solveTreeNode(*treeNode.children[1], &treeNode);
    auto b = buildPlan.toString();
    auto& extraInfo = treeNode.extraInfo->constCast<ExtraJoinTreeNodeInfo>();
    binder::expression_vector joinNodeIDs;
    for (auto& expr : extraInfo.joinNodes) {
        joinNodeIDs.push_back(expr->constCast<NodeExpression>().getInternalID());
    }
    auto plan = LogicalPlan();
    planner->appendHashJoin(joinNodeIDs, JoinType::INNER, probePlan, buildPlan, plan);
    planner->appendFilters(extraInfo.predicates, plan);
    return plan;
}

LogicalPlan JoinPlanSolver::solveMultiwayJoinTreeNode(const JoinTreeNode& treeNode) {
    auto& extraInfo = treeNode.extraInfo->constCast<ExtraJoinTreeNodeInfo>();
    KU_ASSERT(extraInfo.joinNodes.size() == 1);
    auto& joinNode = extraInfo.joinNodes[0]->constCast<NodeExpression>();
    auto probePlan = solveTreeNode(*treeNode.children[0], &treeNode);
    std::vector<std::unique_ptr<LogicalPlan>> buildPlans;
    expression_vector boundNodeIDs;
    for (auto i = 1u; i < treeNode.children.size(); ++i) {
        auto child = treeNode.children[i];
        KU_ASSERT(child->type == JoinNodeType::REL_SCAN);
        auto& childExtraInfo = child->extraInfo->constCast<ExtraScanTreeNodeInfo>();
        KU_ASSERT(childExtraInfo.isSingleRel());
        auto rel = childExtraInfo.relInfos[0].rel;
        auto boundNode = *rel->getSrcNode() == joinNode ? rel->getDstNode() : rel->getSrcNode();
        buildPlans.push_back(solveTreeNode(*child, &treeNode).shallowCopy());
        boundNodeIDs.push_back(boundNode->constCast<NodeExpression>().getInternalID());
    }
    auto plan = LogicalPlan();
    planner->appendIntersect(joinNode.getInternalID(), boundNodeIDs, probePlan, buildPlans);
    plan.setLastOperator(probePlan.getLastOperator()); // TODO: remove this
    planner->appendFilters(extraInfo.predicates, plan);
    return plan;
}

} // namespace planner
} // namespace kuzu
