#pragma once

#include "src/binder/query/include/bound_regular_query.h"
#include "src/planner/include/enumerator.h"

namespace graphflow {
namespace planner {

class Planner {

public:
    static unique_ptr<LogicalPlan> getBestPlan(const Graph& graph, const BoundRegularQuery& query);

    static vector<unique_ptr<LogicalPlan>> getAllPlans(
        const Graph& graph, const BoundRegularQuery& query);

    static unique_ptr<LogicalPlan> getBestPlan(
        const Graph& graph, const NormalizedSingleQuery& query);

    static vector<unique_ptr<LogicalPlan>> getAllPlans(
        const Graph& graph, const NormalizedSingleQuery& query);

private:
    static unique_ptr<LogicalPlan> optimize(unique_ptr<LogicalPlan> plan);

    static vector<vector<unique_ptr<LogicalPlan>>> cartesianProductChildrenPlans(
        vector<vector<unique_ptr<LogicalPlan>>> childrenLogicalPlans);
};

} // namespace planner
} // namespace graphflow
