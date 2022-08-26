#include "src/planner/include/planner.h"

namespace graphflow {
namespace planner {

unique_ptr<LogicalPlan> Planner::getBestPlan(const Catalog& catalog,
    const NodesStatisticsAndDeletedIDs& nodesStatisticsAndDeletedIDs,
    const RelsStatistics& relsStatistics, const BoundStatement& query) {
    return optimize(
        Enumerator(catalog, nodesStatisticsAndDeletedIDs, relsStatistics).getBestPlan(query));
}

vector<unique_ptr<LogicalPlan>> Planner::getAllPlans(const Catalog& catalog,
    const NodesStatisticsAndDeletedIDs& nodesStatisticsAndDeletedIDs,
    const RelsStatistics& relsStatistics, const BoundStatement& query) {
    auto plans =
        Enumerator(catalog, nodesStatisticsAndDeletedIDs, relsStatistics).getAllPlans(query);
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
