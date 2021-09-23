#include "src/planner/include/planner.h"

#include "src/planner/include/property_scan_pushdown.h"

namespace graphflow {
namespace planner {

unique_ptr<LogicalPlan> Planner::getBestPlan(const Graph& graph, const BoundSingleQuery& query) {
    // join order enumeration with filter push down
    auto bestPlan = Enumerator(graph).getBestJoinOrderPlan(query);
    return optimize(move(bestPlan));
}

vector<unique_ptr<LogicalPlan>> Planner::getAllPlans(
    const Graph& graph, const BoundSingleQuery& query) {
    auto plans = Enumerator(graph).getAllPlans(query);
    vector<unique_ptr<LogicalPlan>> optimizedPlans;
    for (auto& plan : plans) {
        optimizedPlans.push_back(optimize(move(plan)));
    }
    return optimizedPlans;
}

unique_ptr<LogicalPlan> Planner::optimize(unique_ptr<LogicalPlan> plan) {
    auto propertyScanPushDown = PropertyScanPushDown();
    plan->lastOperator = propertyScanPushDown.rewrite(plan->lastOperator);
    return plan;
}

} // namespace planner
} // namespace graphflow
