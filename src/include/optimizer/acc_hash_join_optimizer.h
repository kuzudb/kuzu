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

    bool isProbeSideQualified(planner::LogicalOperator* probeRoot);

    std::vector<planner::LogicalOperator*> resolveOperatorsToApplySemiMask(
        const binder::Expression& nodeID, planner::LogicalOperator* root);
    // Find all ScanNodeIDs under root which scans parameter nodeID. Note that there might be
    // multiple ScanNodeIDs matches because both node and rel table scans will trigger scanNodeIDs.
    std::vector<planner::LogicalOperator*> resolveScanInternalIDsToApplySemiMask(
        const binder::Expression& nodeID, planner::LogicalOperator* root);
    // Find all ShortestPathExtend under root which extend to parameter nodeID. There will be at
    // most one match because rel table is scanned exactly once.
    std::vector<planner::LogicalOperator*> resolveShortestPathExtendToApplySemiMask(
        const binder::Expression& nodeID, planner::LogicalOperator* root);

    std::shared_ptr<planner::LogicalOperator> appendNodeSemiMasker(
        std::vector<planner::LogicalOperator*> opsToApplySemiMask,
        std::shared_ptr<planner::LogicalOperator> child);
    std::shared_ptr<planner::LogicalOperator> appendPathSemiMasker(
        const std::shared_ptr<binder::Expression>& pathExpression,
        std::vector<planner::LogicalOperator*> opsToApplySemiMask,
        const std::shared_ptr<planner::LogicalOperator>& child);
    std::shared_ptr<planner::LogicalOperator> appendAccumulate(
        std::shared_ptr<planner::LogicalOperator> child);
};

} // namespace optimizer
} // namespace kuzu
