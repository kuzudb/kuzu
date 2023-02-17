#include "optimizer/optimizer.h"

#include "optimizer/index_nested_loop_join_optimizer.h"
#include "optimizer/factorization_rewriter.h"
#include "optimizer/remove_factorization_rewriter.h"

namespace kuzu {
namespace optimizer {

void Optimizer::optimize(planner::LogicalPlan* plan) {
    auto removeFactorizationRewriter = RemoveFactorizationRewriter();
    removeFactorizationRewriter.rewrite(plan);

    //    IndexNestedLoopJoinOptimizer::rewrite(plan->getLastOperator());

    auto factorizationRewriter = FactorizationRewriter();
    factorizationRewriter.rewrite(plan);
}

} // namespace optimizer
} // namespace kuzu
