#include "src/planner/include/planner.h"

#include "src/planner/include/logical_plan/operator/result_collector/logical_result_collector.h"
#include "src/planner/include/property_scan_pushdown.h"
namespace graphflow {
namespace planner {

unique_ptr<LogicalPlan> Planner::getBestPlan(const Graph& graph, const BoundRegularQuery& query) {
    if (query.getNumSingleQueries() == 1) {
        // The returned logicalPlan doesn't contain the logicalResultCollector as its last operator.
        // We need to append a logicalResultCollector to the logicalPlan.
        auto singleQueryLogicalPlan = getBestPlan(graph, *query.getSingleQuery(0));
        Enumerator::appendLogicalResultCollector(*singleQueryLogicalPlan);
        return singleQueryLogicalPlan;
    } else {
        auto logicalPlan = make_unique<LogicalPlan>();
        vector<unique_ptr<LogicalPlan>> childrenPlans;
        for (auto i = 0u; i < query.getNumSingleQueries(); i++) {
            auto childPlan = getBestPlan(graph, *query.getSingleQuery(i));
            logicalPlan->cost += childPlan->cost;
            childrenPlans.push_back(move(childPlan));
        }
        Enumerator::appendLogicalUnionAndDistinctIfNecessary(
            childrenPlans, *logicalPlan, query.getIsUnionAll(0));
        Enumerator::appendLogicalResultCollector(*logicalPlan);
        return logicalPlan;
    }
}

vector<vector<unique_ptr<LogicalPlan>>> Planner::cartesianProductChildrenPlans(
    vector<vector<unique_ptr<LogicalPlan>>> childrenLogicalPlans) {
    vector<vector<unique_ptr<LogicalPlan>>> resultChildrenPlans;
    for (auto& childLogicalPlans : childrenLogicalPlans) {
        vector<vector<unique_ptr<LogicalPlan>>> curChildResultLogicalPlans;
        for (auto& childLogicalPlan : childLogicalPlans) {
            if (resultChildrenPlans.empty()) {
                vector<unique_ptr<LogicalPlan>> logicalPlans;
                logicalPlans.push_back(childLogicalPlan->deepCopy());
                curChildResultLogicalPlans.push_back(move(logicalPlans));
            } else {
                for (auto& resultChildPlans : resultChildrenPlans) {
                    vector<unique_ptr<LogicalPlan>> logicalPlans;
                    for (auto& resultChildPlan : resultChildPlans) {
                        logicalPlans.push_back(resultChildPlan->deepCopy());
                    }
                    logicalPlans.push_back(childLogicalPlan->deepCopy());
                    curChildResultLogicalPlans.push_back(move(logicalPlans));
                }
            }
        }
        resultChildrenPlans = move(curChildResultLogicalPlans);
    }
    return resultChildrenPlans;
}

vector<unique_ptr<LogicalPlan>> Planner::getAllPlans(
    const Graph& graph, const BoundRegularQuery& query) {
    if (query.getNumSingleQueries() == 1) {
        // The returned logicalPlans don't contain the logicalResultCollector as their last
        // operator. We need to append a logicalResultCollector to each returned logicalPlan.
        auto logicalPlans = getAllPlans(graph, *query.getSingleQuery(0));
        for (auto& logicalPlan : logicalPlans) {
            Enumerator::appendLogicalResultCollector(*logicalPlan);
        }
        return logicalPlans;
    } else {
        vector<vector<unique_ptr<LogicalPlan>>> childrenLogicalPlans(query.getNumSingleQueries());
        for (auto i = 0u; i < query.getNumSingleQueries(); i++) {
            childrenLogicalPlans[i] = getAllPlans(graph, *query.getSingleQuery(i));
        }
        auto childrenPlans = cartesianProductChildrenPlans(move(childrenLogicalPlans));
        vector<unique_ptr<LogicalPlan>> resultPlans;
        for (auto i = 0u; i < childrenPlans.size(); i++) {
            auto& childPlan = childrenPlans[i];
            auto logicalPlan = make_unique<LogicalPlan>();
            for (auto& plan : childPlan) {
                logicalPlan->cost += plan->cost;
            }
            Enumerator::appendLogicalUnionAndDistinctIfNecessary(
                childPlan, *logicalPlan, query.getIsUnionAll(0));
            Enumerator::appendLogicalResultCollector(*logicalPlan);
            resultPlans.push_back(move(logicalPlan));
        }
        return resultPlans;
    }
}

vector<unique_ptr<LogicalPlan>> Planner::getAllPlans(
    const Graph& graph, const NormalizedSingleQuery& query) {
    auto plans = Enumerator(graph).getAllPlans(query);
    vector<unique_ptr<LogicalPlan>> optimizedPlans;
    for (auto& plan : plans) {
        optimizedPlans.push_back(optimize(move(plan)));
    }
    return optimizedPlans;
}

unique_ptr<LogicalPlan> Planner::getBestPlan(
    const Graph& graph, const NormalizedSingleQuery& query) {
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
