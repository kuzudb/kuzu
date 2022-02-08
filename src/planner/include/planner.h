#pragma once

#include "src/binder/include/bound_queries/bound_regular_query.h"
#include "src/planner/include/enumerator.h"

namespace graphflow {
namespace planner {

class Planner {

public:
    static unique_ptr<LogicalPlan> getBestPlan(const Graph& graph, const BoundRegularQuery& query);

    static vector<unique_ptr<LogicalPlan>> getAllPlans(
        const Graph& graph, const BoundRegularQuery& query);

    static unique_ptr<LogicalPlan> getBestPlan(const Graph& graph, const BoundSingleQuery& query);

    static vector<unique_ptr<LogicalPlan>> getAllPlans(
        const Graph& graph, const BoundSingleQuery& query);

private:
    static unique_ptr<LogicalPlan> optimize(unique_ptr<LogicalPlan> plan);

    static vector<vector<unique_ptr<LogicalPlan>>> cartesianProductChildrenPlans(
        vector<vector<unique_ptr<LogicalPlan>>> childrenLogicalPlans);
};

} // namespace planner
} // namespace graphflow
