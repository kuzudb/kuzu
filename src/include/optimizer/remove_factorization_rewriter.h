#include "planner/logical_plan/logical_plan.h"

namespace kuzu {
namespace optimizer {

class RemoveFactorizationRewriter {
public:
    void rewrite(planner::LogicalPlan* plan);

private:
    void visitOperator(planner::LogicalOperator* op);

    std::shared_ptr<planner::LogicalOperator> getNonFlattenOp(std::shared_ptr<planner::LogicalOperator> op);
};

} // namespace optimizer
} // namespace kuzu
