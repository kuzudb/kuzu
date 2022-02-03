#include "src/planner/include/planner.h"

#include "src/planner/include/logical_plan/operator/result_collector/logical_result_collector.h"
#include "src/planner/include/property_scan_pushdown.h"
namespace graphflow {
namespace planner {

unique_ptr<LogicalPlan> Planner::getBestPlan(const Graph& graph, const BoundRegularQuery& query) {
    if (query.getNumBoundSingleQueries() == 1) {
        return getBestPlan(graph, *query.getBoundSingleQuery(0));
    } else {
        auto logicalPlan = make_unique<LogicalPlan>();
        vector<unique_ptr<LogicalPlan>> childrenPlans;
        for (auto i = 0u; i < query.getNumBoundSingleQueries(); i++) {
            auto childPlan = getBestPlan(graph, *query.getBoundSingleQuery(i));
            logicalPlan->cost += childPlan->cost;
            childrenPlans.push_back(move(childPlan));
        }
        Enumerator::appendLogicalUnionAll(childrenPlans, *logicalPlan);
        Enumerator::appendLogicalResultCollector(*logicalPlan);
        return logicalPlan;
    }
}

vector<unique_ptr<LogicalPlan>> Planner::getAllPlans(
    const Graph& graph, const BoundRegularQuery& query) {
    if (query.getNumBoundSingleQueries() == 1) {
        return getAllPlans(graph, *query.getBoundSingleQuery(0));
    } else {
        // For regular queries, we don't do cartesian product on all sub-plans. We just return the
        // best plan for regular queries.
        vector<unique_ptr<LogicalPlan>> logicalPlans;
        logicalPlans.push_back(getBestPlan(graph, query));
        return logicalPlans;
    }
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

unique_ptr<LogicalPlan> Planner::getBestPlan(const Graph& graph, const BoundSingleQuery& query) {
    // join order enumeration with filter push down
    auto bestPlan = Enumerator(graph).getBestJoinOrderPlan(query);
    return optimize(move(bestPlan));
}

unique_ptr<LogicalPlan> Planner::optimize(unique_ptr<LogicalPlan> plan) {
    auto propertyScanPushDown = PropertyScanPushDown();
    propertyScanPushDown.rewrite(*plan);
    return plan;
}

} // namespace planner
} // namespace graphflow
