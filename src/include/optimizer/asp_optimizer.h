#include "logical_operator_visitor.h"
#include "planner/logical_plan/logical_plan.h"

namespace kuzu {
namespace optimizer {

// This optimizer implements the ASP join algorithm as introduced in paper "Kùzu Graph Database
// Management System".
class ASPOptimizer : public LogicalOperatorVisitor {
public:
    void rewrite(planner::LogicalPlan* plan);

private:
    void visitOperator(planner::LogicalOperator* op);

    void visitHashJoin(planner::LogicalOperator* op) override;

    bool canApplyASP(planner::LogicalOperator* op);
};

} // namespace optimizer
} // namespace kuzu
