#pragma once

#include "src/planner/include/enumerator.h"

namespace graphflow {
namespace planner {

class Planner {
public:
    static unique_ptr<LogicalPlan> getBestPlan(const Catalog& catalog,
        const NodesStatisticsAndDeletedIDs& nodesStatistics, const RelsStatistics& relsStatistics,
        const BoundStatement& statement);

    static vector<unique_ptr<LogicalPlan>> getAllPlans(const Catalog& catalog,
        const NodesStatisticsAndDeletedIDs& nodesStatistics, const RelsStatistics& relsStatistics,
        const BoundStatement& statement);

private:
    static unique_ptr<LogicalPlan> planCreateNodeTable(const BoundStatement& statement);

    static unique_ptr<LogicalPlan> planCreateRelTable(const BoundStatement& statement);

    static unique_ptr<LogicalPlan> planDropTable(const BoundStatement& statement);

    static unique_ptr<LogicalPlan> planCopyCSV(const BoundStatement& statement);
};

} // namespace planner
} // namespace graphflow
