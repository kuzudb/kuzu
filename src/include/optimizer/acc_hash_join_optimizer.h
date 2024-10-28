#pragma once

#include "logical_operator_visitor.h"
#include "planner/operator/logical_plan.h"

namespace kuzu {
namespace optimizer {

// This optimizer enables the Accumulated hash join algorithm as introduced in paper "Kuzu Graph
// Database Management System".
class HashJoinSIPOptimizer : public LogicalOperatorVisitor {
public:
    void rewrite(planner::LogicalPlan* plan);

private:
    void visitOperator(planner::LogicalOperator* op);

    void visitHashJoin(planner::LogicalOperator* op) override;

    bool tryProbeToBuildHJSIP(planner::LogicalOperator* op);
    bool tryBuildToProbeHJSIP(planner::LogicalOperator* op);

    void visitIntersect(planner::LogicalOperator* op) override;

    void visitPathPropertyProbe(planner::LogicalOperator* op) override;

    std::shared_ptr<planner::LogicalOperator> tryApplySemiMask(
        std::shared_ptr<binder::Expression> nodeID,
        std::shared_ptr<planner::LogicalOperator> fromRoot, planner::LogicalOperator* toRoot);
    // Find all ScanNodeIDs under root which scans parameter nodeID. Note that there might be
    // multiple ScanNodeIDs matches because both node and rel table scans will trigger scanNodeIDs.
    std::vector<planner::LogicalOperator*> getScanNodeCandidates(const binder::Expression& nodeID,
        planner::LogicalOperator* root);
    // Find all ShortestPathExtend under root which extend to parameter nodeID. There will be at
    // most one match because rel table is scanned exactly once.
    std::vector<planner::LogicalOperator*> getRecursiveJoinCandidates(
        const binder::Expression& nodeID, planner::LogicalOperator* root);
    std::vector<planner::LogicalOperator*> getGDSCallInputNodeCandidates(
        const binder::Expression& nodeID, planner::LogicalOperator* root);
    std::vector<planner::LogicalOperator*> getGDSCallOutputNodeCandidates(
        const binder::Expression& nodeID, planner::LogicalOperator* root);
};

} // namespace optimizer
} // namespace kuzu
