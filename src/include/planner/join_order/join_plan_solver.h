#pragma once

#include "join_tree.h"
#include "planner/planner.h"

namespace kuzu {
namespace planner {

class JoinPlanSolver {
public:
    explicit JoinPlanSolver(Planner* planner) : planner{planner} {}

    LogicalPlan solve(const JoinTree& joinTree);

private:
    LogicalPlan solveTreeNode(const JoinTreeNode& current, const JoinTreeNode* parent);

    LogicalPlan solveNodeScanTreeNode(const JoinTreeNode& treeNode) const;
    LogicalPlan solveRelScanTreeNode(const JoinTreeNode& treeNode, const JoinTreeNode& parent) const;
    LogicalPlan solveBinaryJoinTreeNode(const JoinTreeNode& treeNode);
    LogicalPlan solveMultiwayJoinTreeNode(const JoinTreeNode& treeNode);

private:
    Planner* planner;
};

} // namespace planner
} // namespace kuzu
