#pragma once

#include "planner/planner.h"
#include "join_tree.h"

namespace kuzu {
namespace planner {

/*
 * JoinPlanSolver solves a JoinTree into a LogicalPlan
 * */
class JoinPlanSolver {
public:
    JoinPlanSolver(Planner* planner) : planner{planner} {}

    LogicalPlan solve(const JoinTree& joinTree);

private:
    LogicalPlan solveTreeNode(const JoinTreeNode& current, const JoinTreeNode* parent);

    LogicalPlan solveExprScanTreeNode(const JoinTreeNode& treeNode);
    LogicalPlan solveNodeScanTreeNode(const JoinTreeNode& treeNode);
    LogicalPlan solveRelScanTreeNode(const JoinTreeNode& treeNode, const JoinTreeNode& parent);
    LogicalPlan solveBinaryJoinTreeNode(const JoinTreeNode& treeNode);
    LogicalPlan solveMultiwayJoinTreeNode(const JoinTreeNode& treeNode);

private:
    Planner* planner;
};

}
}
