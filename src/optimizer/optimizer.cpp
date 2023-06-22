#include "optimizer/optimizer.h"

#include "optimizer/acc_hash_join_optimizer.h"
#include "optimizer/agg_key_dependency_optimizer.h"
#include "optimizer/factorization_rewriter.h"
#include "optimizer/filter_push_down_optimizer.h"
#include "optimizer/projection_push_down_optimizer.h"
#include "optimizer/remove_factorization_rewriter.h"
#include "optimizer/remove_unnecessary_join_optimizer.h"

namespace kuzu {
namespace optimizer {

void Optimizer::optimize(planner::LogicalPlan* plan) {
    // Factorization structure should be removed before further optimization can be applied.
    auto removeFactorizationRewriter = RemoveFactorizationRewriter();
    removeFactorizationRewriter.rewrite(plan);

    auto removeUnnecessaryJoinOptimizer = RemoveUnnecessaryJoinOptimizer();
    removeUnnecessaryJoinOptimizer.rewrite(plan);

    auto filterPushDownOptimizer = FilterPushDownOptimizer();
    filterPushDownOptimizer.rewrite(plan);

    auto projectionPushDownOptimizer = ProjectionPushDownOptimizer();
    projectionPushDownOptimizer.rewrite(plan);

    // HashJoinSIPOptimizer should be applied after optimizers that manipulate hash join.
    auto hashJoinSIPOptimizer = HashJoinSIPOptimizer();
    hashJoinSIPOptimizer.rewrite(plan);

    auto factorizationRewriter = FactorizationRewriter();
    factorizationRewriter.rewrite(plan);

    // AggKeyDependencyOptimizer doesn't change factorization structure and thus can be put after
    // FactorizationRewriter.
    auto aggKeyDependencyOptimizer = AggKeyDependencyOptimizer();
    aggKeyDependencyOptimizer.rewrite(plan);
}

} // namespace optimizer
} // namespace kuzu
