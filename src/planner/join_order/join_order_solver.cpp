#include "planner/join_order/join_order_solver.h"

#include "planner/join_order/cost_model.h"
#include "planner/planner.h"
#include "storage/storage_manager.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

JoinTree JoinOrderSolver::solve() {
    auto nStats = context->getStorageManager()->getNodesStatisticsAndDeletedIDs();
    auto rStats = context->getStorageManager()->getRelsStatistics();
    cardinalityEstimator = CardinalityEstimator(context, nStats, rStats);
    cardinalityEstimator.initNodeIDDom(queryGraph, context->getTx());
    auto currentLevel = 1u;
    auto maxLevel = queryGraph.getNumQueryNodes() + queryGraph.getNumQueryRels();
    dpTable.init(maxLevel);
    planBaseScans();
    currentLevel++;
    while (currentLevel <= maxLevel) {
        planLevel(currentLevel++);
    }
    auto& lastLevel = dpTable.getLevel(maxLevel);
    KU_ASSERT(lastLevel.getSubgraphAndJoinTrees().size() == 1);
    return lastLevel.getSubgraphAndJoinTrees().begin()->second;
}

void JoinOrderSolver::planLevel(common::idx_t level) {
    auto maxLeftLevel = floor(level / 2.0);
    for (auto leftLevel = 1u; leftLevel <= maxLeftLevel; ++leftLevel) {
        auto rightLevel = level - leftLevel;
        planWorstCaseOptimalJoin(leftLevel, rightLevel);
        planBinaryJoin(leftLevel, rightLevel);
        // TODO: solve approximately
    }
}

void JoinOrderSolver::planBaseScans() {
    if (subqueryType == SubqueryType::CORRELATED) {
        auto correlatedExpressionSet = expression_set{corrExprs.begin(), corrExprs.end()};
        auto newSubgraph = SubqueryGraph(queryGraph);
        for (auto i = 0u; i < queryGraph.getNumQueryNodes(); ++i) {
            auto node = queryGraph.getQueryNode(i);
            if (correlatedExpressionSet.contains(node->getInternalID())) {
                newSubgraph.addQueryNode(i);
                continue;
            }
            planBaseNodeScan(i);
        }
        planCorrelatedExpressionsScan(newSubgraph);
    } else {
        for (auto i = 0u; i < queryGraph.getNumQueryNodes(); ++i) {
            planBaseNodeScan(i);
        }
    }
    for (auto i = 0u; i < queryGraph.getNumQueryRels(); ++i) {
        planBaseRelScan(i);
    }
}

void JoinOrderSolver::planCorrelatedExpressionsScan(const binder::SubqueryGraph& newSubgraph) {
    auto emptySubgraph = SubqueryGraph(queryGraph);
    auto predicates =
        Planner::getNewlyMatchedExpressions(emptySubgraph, newSubgraph, queryGraphPredicates);
    auto extraInfo = std::make_unique<ExtraExprScanTreeNodeInfo>(corrExprs);
    extraInfo->predicates = predicates;
    auto joinNode =
        std::make_shared<JoinTreeNode>(JoinNodeType::EXPRESSION_SCAN, std::move(extraInfo));
    auto joinTree = JoinTree(joinNode);
    // Estimate cost & cardinality.
    auto estCardinality = corrExprsCardinality;
    estCardinality = cardinalityEstimator.estimateFilters(estCardinality, predicates);
    joinTree.cardinality = estCardinality;
    joinTree.cost = estCardinality;
    // Insert to dp table.
    dpTable.add(newSubgraph, joinTree);
}

void JoinOrderSolver::planBaseNodeScan(common::idx_t nodeIdx) {
    auto emptySubgraph = SubqueryGraph(queryGraph);
    auto newSubgraph = SubqueryGraph(queryGraph);
    newSubgraph.addQueryNode(nodeIdx);
    auto node = queryGraph.getQueryNode(nodeIdx);
    auto properties = propertyCollection.getProperties(node);
    auto predicates =
        Planner::getNewlyMatchedExpressions(emptySubgraph, newSubgraph, queryGraphPredicates);
    auto scanInfo = std::make_unique<NodeTableScanInfo>(node, properties);
    scanInfo->predicates = predicates;
    auto extraInfo = std::make_unique<ExtraScanTreeNodeInfo>();
    extraInfo->nodeInfo = std::move(scanInfo);
    auto joinNode = std::make_shared<JoinTreeNode>(JoinNodeType::NODE_SCAN, std::move(extraInfo));
    auto joinTree = JoinTree(joinNode);
    // Estimate cost & cardinality.
    auto estCardinality = cardinalityEstimator.getNumNodes(node->getTableIDs(), context->getTx());
    estCardinality = cardinalityEstimator.estimateFilters(estCardinality, predicates);
    joinTree.cardinality = estCardinality;
    joinTree.cost = estCardinality;
    // Insert to dp table.
    dpTable.add(newSubgraph, joinTree);
}

void JoinOrderSolver::planBaseRelScan(common::idx_t relIdx) {
    auto emptySubgraph = SubqueryGraph(queryGraph);
    auto subgraph = SubqueryGraph(queryGraph);
    subgraph.addQueryRel(relIdx);
    auto rel = queryGraph.getQueryRel(relIdx);
    auto properties = propertyCollection.getProperties(rel);
    auto scanInfo = RelTableScanInfo(rel, properties);
    auto predicates =
        Planner::getNewlyMatchedExpressions(emptySubgraph, subgraph, queryGraphPredicates);
    scanInfo.predicates = predicates;
    auto extraInfo = std::make_unique<ExtraScanTreeNodeInfo>();
    extraInfo->relInfos.push_back(std::move(scanInfo));
    auto joinNode = std::make_shared<JoinTreeNode>(JoinNodeType::REL_SCAN, std::move(extraInfo));
    auto joinTree = JoinTree(joinNode);
    // Estimate cost & cardinality.
    auto estCardinality = cardinalityEstimator.getNumRels(rel->getTableIDs(), context->getTx());
    estCardinality = cardinalityEstimator.estimateFilters(estCardinality, predicates);
    joinTree.cardinality = estCardinality;
    joinTree.cost = estCardinality;
    // Insert to dp table.
    dpTable.add(subgraph, joinTree);
}
// E.g. Query graph (a)-[e1]->(b), (b)-[e2]->(a) and join between (a)-[e1] and [e2]
// Since (b) is not in the scope of any join subgraph, join node is analyzed as (a) only, However,
// [e1] and [e2] are also connected at (b) implicitly. So actual join nodes should be (a) and (b).
// We prune such join.
// Note that this does not mean we may lose good plan. An equivalent join can be found between [e2]
// and (a)-[e1]->(b).
static bool needPruneImplicitJoins(const SubqueryGraph& leftSubgraph,
    const SubqueryGraph& rightSubgraph, uint32_t numJoinNodes) {
    auto leftNodePositions = leftSubgraph.getNodePositionsIgnoringNodeSelector();
    auto rightNodePositions = rightSubgraph.getNodePositionsIgnoringNodeSelector();
    auto intersectionSize = 0u;
    for (auto& pos : leftNodePositions) {
        if (rightNodePositions.contains(pos)) {
            intersectionSize++;
        }
    }
    return intersectionSize != numJoinNodes;
}

void JoinOrderSolver::planBinaryJoin(common::idx_t leftSize, common::idx_t rightSize) {
    auto& leftLevel = dpTable.getLevel(leftSize);
    auto& rightLevel = dpTable.getLevel(rightSize);
    // Foreach subgraph in the right dp level
    for (auto& [rightSubgraph, rightJoinTree] : rightLevel.getSubgraphAndJoinTrees()) {
        // Find all connected subgraphs with number of nodes equals to leftNumNodes
        for (auto& leftSubgraph : rightSubgraph.getNbrSubgraphs(leftSize)) {
            // TODO(Xiyang): Ideally we don't want to perform the following check. Every subgraph
            // should exist in the dp table.
            if (!leftLevel.contains(leftSubgraph)) {
                continue;
            }
            auto joinNodePositions = rightSubgraph.getConnectedNodePos(leftSubgraph);
            // TODO(Xiyang): try to remove
            if (needPruneImplicitJoins(leftSubgraph, rightSubgraph, joinNodePositions.size())) {
                continue;
            }
            auto joinNodes = queryGraph.getQueryNodes(joinNodePositions);
            auto& leftJoinTree = leftLevel.getJoinTree(leftSubgraph);
            planBinaryJoin(leftSubgraph, leftJoinTree, rightSubgraph, rightJoinTree, joinNodes);
        }
    }
}

void JoinOrderSolver::planWorstCaseOptimalJoin(common::idx_t size, common::idx_t otherSize) {
    if (size == 1) {
        return;
    }
    KU_ASSERT(size <= otherSize);
    auto otherLevel = dpTable.getLevel(otherSize);
    for (auto& [otherSubgraph, otherJoinTree] : otherLevel.getSubgraphAndJoinTrees()) {
        auto candidates = otherSubgraph.getWCOJRelCandidates();
        for (auto& [nodeIdx, relIndices] : candidates) {
            if (relIndices.size() != size) {
                continue;
            }
            auto joinNode = queryGraph.getQueryNode(nodeIdx);

            std::vector<JoinTree> relJoinTrees;
            std::vector<SubqueryGraph> prevSubqueryGraphs;
            prevSubqueryGraphs.push_back(otherSubgraph);
            auto newSubqueryGraph = otherSubgraph;
            for (auto& relIdx : relIndices) {
                auto subgraph = SubqueryGraph(queryGraph);
                subgraph.addQueryRel(relIdx);
                prevSubqueryGraphs.push_back(subgraph);
                newSubqueryGraph.addSubqueryGraph(subgraph);
                auto& relJoinTree = dpTable.getLevel(1).getJoinTree(subgraph);
                relJoinTrees.push_back(relJoinTree);
            }

            auto predicates = Planner::getNewlyMatchedExpressions(prevSubqueryGraphs,
                newSubqueryGraph, queryGraphPredicates);
            planWorstCaseOptimalJoin(otherJoinTree, relJoinTrees, joinNode, newSubqueryGraph,
                predicates);
        }
    }
}

void JoinOrderSolver::planBinaryJoin(const SubqueryGraph& subqueryGraph, const JoinTree& joinTree,
    const binder::SubqueryGraph& otherSubqueryGraph, const JoinTree& otherJoinTree,
    std::vector<std::shared_ptr<NodeExpression>> joinNodes) {
    auto newSubgraph = subqueryGraph;
    newSubgraph.addSubqueryGraph(otherSubqueryGraph);
    auto predicates = Planner::getNewlyMatchedExpressions(subqueryGraph, otherSubqueryGraph,
        newSubgraph, queryGraphPredicates);
    // First try to solve index nested loop join. These joins, if doable, are preferred over hash
    // join because we have a built-in CSR index.
    if (joinNodes.size() == 1 && tryPlanIndexNestedLoopJoin(joinTree, otherJoinTree, joinNodes[0],
                                     newSubgraph, predicates)) {
        return;
    }
    planHashJoin(joinTree, otherJoinTree, joinNodes, newSubgraph, predicates);
}

void JoinOrderSolver::planHashJoin(const JoinTree& joinTree, const JoinTree& otherJoinTree,
    std::vector<std::shared_ptr<binder::NodeExpression>> joinNodes,
    const SubqueryGraph& newSubqueryGraph, const expression_vector& predicates) {
    if (joinTree.cardinality < otherJoinTree.cardinality) {
        planHashJoin(otherJoinTree, joinTree, joinNodes, newSubqueryGraph, predicates);
    }
    auto extraInfo = std::make_unique<ExtraJoinTreeNodeInfo>(joinNodes);
    extraInfo->predicates = predicates;
    auto treeNode = std::make_shared<JoinTreeNode>(JoinNodeType::BINARY_JOIN, std::move(extraInfo));
    treeNode->addChild(joinTree.root);
    treeNode->addChild(otherJoinTree.root);
    auto newJoinTree = JoinTree(treeNode);
    // Estimate cost & cardinality.
    newJoinTree.cost = CostModel::computeHashJoinCost(joinTree.cost, otherJoinTree.cost,
        joinTree.cardinality, otherJoinTree.cardinality);
    binder::expression_vector joinNodeIDs;
    for (auto& joinNode : joinNodes) {
        joinNodeIDs.push_back(joinNode->getInternalID());
    }
    auto estCardinality = cardinalityEstimator.estimateHashJoin(joinNodeIDs, joinTree.cardinality,
        otherJoinTree.cardinality);
    estCardinality = cardinalityEstimator.estimateFilters(estCardinality, predicates);
    newJoinTree.cardinality = estCardinality;
    // Insert to dp table.
    dpTable.add(newSubqueryGraph, newJoinTree);
}

void JoinOrderSolver::planWorstCaseOptimalJoin(const JoinTree& joinTree,
    const std::vector<JoinTree>& relJoinTrees, std::shared_ptr<NodeExpression> joinNode,
    const SubqueryGraph& newSubqueryGraph, const binder::expression_vector& predicates) {
    std::vector<std::shared_ptr<binder::NodeExpression>> joinNodes;
    joinNodes.push_back(joinNode);
    auto extraInfo = std::make_unique<ExtraJoinTreeNodeInfo>(joinNodes);
    extraInfo->predicates = predicates;
    auto treeNode =
        std::make_shared<JoinTreeNode>(JoinNodeType::MULTIWAY_JOIN, std::move(extraInfo));
    treeNode->addChild(joinTree.root);
    for (auto& relJoinTree : relJoinTrees) {
        treeNode->addChild(relJoinTree.root);
    }
    auto newJoinTree = JoinTree(treeNode);
    // Estimate cost & cardinality.
    std::vector<cost_t> buildCosts;
    std::vector<cardianlity_t> buildCards;
    for (auto& relJoinTree : relJoinTrees) {
        buildCosts.push_back(relJoinTree.cost);
        buildCards.push_back(relJoinTree.cardinality);
    }
    newJoinTree.cost =
        CostModel::computeIntersectCost(joinTree.cost, buildCosts, joinTree.cardinality);
    binder::expression_vector joinNodeIDs;
    joinNodeIDs.push_back(joinNode->getInternalID());
    auto estCardinality =
        cardinalityEstimator.estimateIntersect(joinNodeIDs, joinTree.cardinality, buildCards);
    estCardinality = cardinalityEstimator.estimateFilters(estCardinality, predicates);
    newJoinTree.cardinality = estCardinality;
    // Insert to dp table.
    dpTable.add(newSubqueryGraph, newJoinTree);
}

bool JoinOrderSolver::tryPlanIndexNestedLoopJoin(const JoinTree& joinTree,
    const JoinTree& otherJoinTree, std::shared_ptr<NodeExpression> joinNode,
    const SubqueryGraph& newSubqueryGraph, const binder::expression_vector& predicates) {
    if (!joinTree.isSingleRel() && !otherJoinTree.isSingleRel()) {
        return false;
    }
    if (joinTree.isSingleRel()) {
        return tryPlanIndexNestedLoopJoin(otherJoinTree, joinTree, joinNode, newSubqueryGraph,
            predicates);
    }
    if (joinTree.root->type != JoinNodeType::NODE_SCAN) {
        return false;
    }
    auto& extraInfo = joinTree.root->extraInfo->constCast<ExtraScanTreeNodeInfo>();
    auto& otherExtraInfo = otherJoinTree.root->extraInfo->constCast<ExtraScanTreeNodeInfo>();
    KU_ASSERT(otherExtraInfo.relInfos.size() == 1);
    auto rel = otherExtraInfo.relInfos[0].rel;
    if (extraInfo.nodeInfo != nullptr && *extraInfo.nodeInfo->node == *joinNode) {
        auto newExtraScanInfo = extraInfo.copy();
        auto& newExtraNodeScanInfo = newExtraScanInfo->cast<ExtraScanTreeNodeInfo>();
        newExtraNodeScanInfo.merge(otherExtraInfo);
        for (auto& predicate : predicates) {
            newExtraNodeScanInfo.predicates.push_back(predicate);
        }
        auto treeNode =
            std::make_shared<JoinTreeNode>(JoinNodeType::NODE_SCAN, std::move(newExtraScanInfo));
        auto newJoinTree = JoinTree(treeNode);
        // Estimate cost & cardinality.
        auto extensionRate =
            cardinalityEstimator.getExtensionRate(*rel, *joinNode, context->getTx());
        if (rel->isRecursive()) {
            newJoinTree.cost = CostModel::computeRecursiveExtendCost(joinTree.cardinality,
                rel->getUpperBound(), extensionRate);
        } else {
            newJoinTree.cost = CostModel::computeExtendCost(joinTree.cardinality);
        }
        auto estCardinality = joinTree.cardinality * extensionRate;
        estCardinality = cardinalityEstimator.estimateFilters(estCardinality, predicates);
        newJoinTree.cardinality = estCardinality;
        // Insert to dp table.
        dpTable.add(newSubqueryGraph, newJoinTree);
        return true;
    }
    return false;
}

} // namespace planner
} // namespace kuzu
