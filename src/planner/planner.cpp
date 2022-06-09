#include "src/planner/include/planner.h"

namespace graphflow {
namespace planner {

unique_ptr<LogicalPlan> Planner::getBestPlan(
    const Catalog& catalog, const NodesMetadata& nodesMetadata, const BoundRegularQuery& query) {
    return optimize(Enumerator(catalog, nodesMetadata).getBestPlan(query));
}

vector<unique_ptr<LogicalPlan>> Planner::getAllPlans(
    const Catalog& catalog, const NodesMetadata& nodesMetadata, const BoundRegularQuery& query) {
    auto plans = Enumerator(catalog, nodesMetadata).getAllPlans(query);
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
