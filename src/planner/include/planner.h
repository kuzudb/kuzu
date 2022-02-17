#pragma once

#include "src/planner/include/enumerator.h"

namespace graphflow {
namespace planner {

class Planner {

public:
    static unique_ptr<LogicalPlan> getBestPlan(const Graph& graph, const BoundRegularQuery& query);

    static vector<unique_ptr<LogicalPlan>> getAllPlans(
        const Graph& graph, const BoundRegularQuery& query);

private:
    static unique_ptr<LogicalPlan> optimize(unique_ptr<LogicalPlan> plan);
};

} // namespace planner
} // namespace graphflow
