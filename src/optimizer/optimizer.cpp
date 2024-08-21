#include "optimizer/optimizer.h"

#include "main/client_context.h"
#include "optimizer/acc_hash_join_optimizer.h"
#include "optimizer/agg_key_dependency_optimizer.h"
#include "optimizer/correlated_subquery_unnest_solver.h"
#include "optimizer/factorization_rewriter.h"
#include "optimizer/filter_push_down_optimizer.h"
#include "optimizer/projection_push_down_optimizer.h"
#include "optimizer/remove_factorization_rewriter.h"
#include "optimizer/remove_unnecessary_join_optimizer.h"
#include "optimizer/top_k_optimizer.h"

namespace kuzu {
namespace optimizer {

void Optimizer::optimize(planner::LogicalPlan* plan, main::ClientContext* context) {
    // Factorization structure should be removed before further optimization can be applied.
    auto removeFactorizationRewriter = RemoveFactorizationRewriter();
    removeFactorizationRewriter.rewrite(plan);

    auto correlatedSubqueryUnnestSolver = CorrelatedSubqueryUnnestSolver(nullptr);
    correlatedSubqueryUnnestSolver.solve(plan->getLastOperator().get());

    auto removeUnnecessaryJoinOptimizer = RemoveUnnecessaryJoinOptimizer();
    removeUnnecessaryJoinOptimizer.rewrite(plan);

    auto filterPushDownOptimizer = FilterPushDownOptimizer(context);
    filterPushDownOptimizer.rewrite(plan);

    auto projectionPushDownOptimizer = ProjectionPushDownOptimizer();
    projectionPushDownOptimizer.rewrite(plan);

    if (context->getClientConfig()->enableSemiMask) {
        // HashJoinSIPOptimizer should be applied after optimizers that manipulate hash join.
        auto hashJoinSIPOptimizer = HashJoinSIPOptimizer();
        hashJoinSIPOptimizer.rewrite(plan);
    }

    auto topKOptimizer = TopKOptimizer();
    topKOptimizer.rewrite(plan);

    auto factorizationRewriter = FactorizationRewriter();
    factorizationRewriter.rewrite(plan);

    // AggKeyDependencyOptimizer doesn't change factorization structure and thus can be put after
    // FactorizationRewriter.
    auto aggKeyDependencyOptimizer = AggKeyDependencyOptimizer();
    aggKeyDependencyOptimizer.rewrite(plan);
}

} // namespace optimizer
} // namespace kuzu
