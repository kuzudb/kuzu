#include "src/planner/include/planner.h"

#include "src/planner/logical_plan/logical_operator/include/logical_result_collector.h"

namespace graphflow {
namespace planner {

unique_ptr<LogicalPlan> Planner::getBestPlan(const Graph& graph, const BoundRegularQuery& query) {
    return optimize(Enumerator(graph).getBestPlan(query));
}

vector<unique_ptr<LogicalPlan>> Planner::getAllPlans(
    const Graph& graph, const BoundRegularQuery& query) {
    auto plans = Enumerator(graph).getAllPlans(query);
    vector<unique_ptr<LogicalPlan>> optimizedPlans;
    for (auto& plan : plans) {
        optimizedPlans.push_back(optimize(move(plan)));
    }
    return optimizedPlans;
}

unique_ptr<LogicalPlan> Planner::optimize(unique_ptr<LogicalPlan> plan) {
    return plan;
}

} // namespace planner
} // namespace graphflow
