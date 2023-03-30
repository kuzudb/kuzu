#include "logical_operator_visitor.h"
#include "planner/logical_plan/logical_plan.h"

namespace kuzu {
namespace optimizer {

// This optimizer enables the Accumulated hash join algorithm as introduced in paper "Kuzu Graph
// Database Management System".
class AccHashJoinOptimizer : public LogicalOperatorVisitor {
public:
    void rewrite(planner::LogicalPlan* plan);

private:
    void visitOperator(planner::LogicalOperator* op);

    void visitHashJoin(planner::LogicalOperator* op) override;

    bool tryProbeToBuildHJSIP(planner::LogicalOperator* op);
    bool tryBuildToProbeHJSIP(planner::LogicalOperator* op);

    void visitIntersect(planner::LogicalOperator* op) override;

    bool isProbeSideQualified(planner::LogicalOperator* probeRoot);

    binder::expression_map<std::vector<planner::LogicalOperator*>> resolveScanNodesToApplySemiMask(
        const binder::expression_vector& nodeIDCandidates,
        const std::vector<planner::LogicalOperator*>& roots);

    std::shared_ptr<planner::LogicalOperator> applySemiMasks(
        const binder::expression_map<std::vector<planner::LogicalOperator*>>& nodeIDToScanNodes,
        std::shared_ptr<planner::LogicalOperator> root);
    void applyAccHashJoin(
        const binder::expression_map<std::vector<planner::LogicalOperator*>>& nodeIDToScanNodes,
        planner::LogicalOperator* op);
};

} // namespace optimizer
} // namespace kuzu
