#include "optimizer/optimizer.h"

#include "optimizer/factorization_rewriter.h"
#include "optimizer/index_nested_loop_join_optimizer.h"
#include "optimizer/projection_push_down_optimizer.h"
#include "optimizer/remove_factorization_rewriter.h"
#include "optimizer/remove_unnecessary_join_optimizer.h"

namespace kuzu {
namespace optimizer {

void Optimizer::optimize(planner::LogicalPlan* plan) {
    auto removeFactorizationRewriter = RemoveFactorizationRewriter();
    removeFactorizationRewriter.rewrite(plan);

    auto removeUnnecessaryJoinOptimizer = RemoveUnnecessaryJoinOptimizer();
    removeUnnecessaryJoinOptimizer.rewrite(plan);

    auto indexNestedLoopJoinOptimizer = IndexNestedLoopJoinOptimizer();
    indexNestedLoopJoinOptimizer.rewrite(plan);

    auto projectionPushDownOptimizer = ProjectionPushDownOptimizer();
    projectionPushDownOptimizer.rewrite(plan);

    auto factorizationRewriter = FactorizationRewriter();
    factorizationRewriter.rewrite(plan);
}

} // namespace optimizer
} // namespace kuzu
