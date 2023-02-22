#include "planner/logical_plan/logical_plan.h"

namespace kuzu {
namespace optimizer {

class FactorizationRewriter {
public:
    void rewrite(planner::LogicalPlan* plan);

private:
    void visitOperator(planner::LogicalOperator* op);
    void visitExtend(planner::LogicalOperator* op);
    void visitHashJoin(planner::LogicalOperator* op);
    void visitIntersect(planner::LogicalOperator* op);
    void visitProjection(planner::LogicalOperator* op);
    void visitAggregate(planner::LogicalOperator* op);
    void visitOrderBy(planner::LogicalOperator* op);
    void visitSkip(planner::LogicalOperator* op);
    void visitLimit(planner::LogicalOperator* op);
    void visitDistinct(planner::LogicalOperator* op);
    void visitUnwind(planner::LogicalOperator* op);
    void visitUnion(planner::LogicalOperator* op);
    void visitFilter(planner::LogicalOperator* op);
    void visitSetNodeProperty(planner::LogicalOperator* op);
    void visitSetRelProperty(planner::LogicalOperator* op);
    void visitDeleteRel(planner::LogicalOperator* op);
    void visitCreateNode(planner::LogicalOperator* op);
    void visitCreateRel(planner::LogicalOperator* op);

    std::shared_ptr<planner::LogicalOperator> appendFlattens(
        std::shared_ptr<planner::LogicalOperator> op,
        const std::unordered_set<planner::f_group_pos>& groupsPos);
    std::shared_ptr<planner::LogicalOperator> appendFlattenIfNecessary(
        std::shared_ptr<planner::LogicalOperator> op, planner::f_group_pos groupPos);
};

} // namespace optimizer
} // namespace kuzu
