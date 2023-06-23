#pragma once

#include "planner/query_planner.h"

namespace kuzu {
namespace planner {

class Planner {
public:
    static std::unique_ptr<LogicalPlan> getBestPlan(const catalog::Catalog& catalog,
        const storage::NodesStatisticsAndDeletedIDs& nodesStatistics,
        const storage::RelsStatistics& relsStatistics, const BoundStatement& statement);

    static std::vector<std::unique_ptr<LogicalPlan>> getAllPlans(const catalog::Catalog& catalog,
        const storage::NodesStatisticsAndDeletedIDs& nodesStatistics,
        const storage::RelsStatistics& relsStatistics, const BoundStatement& statement);

private:
    static std::unique_ptr<LogicalPlan> planCreateNodeTable(const BoundStatement& statement);

    static std::unique_ptr<LogicalPlan> planCreateRelTable(const BoundStatement& statement);

    static std::unique_ptr<LogicalPlan> planDropTable(const BoundStatement& statement);

    static std::unique_ptr<LogicalPlan> planRenameTable(const BoundStatement& statement);

    static std::unique_ptr<LogicalPlan> planAddProperty(const BoundStatement& statement);

    static std::unique_ptr<LogicalPlan> planDropProperty(const BoundStatement& statement);

    static std::unique_ptr<LogicalPlan> planRenameProperty(const BoundStatement& statement);

    static std::unique_ptr<LogicalPlan> planCopy(
        const catalog::Catalog& catalog, const BoundStatement& statement);

    static std::unique_ptr<LogicalPlan> planCall(const BoundStatement& statement);
};

} // namespace planner
} // namespace kuzu
