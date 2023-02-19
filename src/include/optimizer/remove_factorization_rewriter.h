#include "planner/logical_plan/logical_plan.h"

namespace kuzu {
namespace optimizer {

class RemoveFactorizationRewriter {
public:
    void rewrite(planner::LogicalPlan* plan);

private:
    std::shared_ptr<planner::LogicalOperator> rewriteOperator(
        std::shared_ptr<planner::LogicalOperator> op);

    bool subPlanHasFlatten(planner::LogicalOperator* op);
};

} // namespace optimizer
} // namespace kuzu
