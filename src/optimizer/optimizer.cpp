#include "optimizer/optimizer.h"

#include "optimizer/index_nested_loop_join_optimizer.h"

namespace kuzu {
namespace optimizer {

void Optimizer::optimize(planner::LogicalPlan* plan) {
    IndexNestedLoopJoinOptimizer::rewrite(plan->getLastOperator());
}

} // namespace optimizer
} // namespace kuzu
