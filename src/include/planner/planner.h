#pragma once

#include "binder/bound_statement.h"
#include "catalog/catalog.h"
#include "planner/operator/logical_plan.h"
#include "storage/stats/nodes_store_statistics.h"
#include "storage/stats/rels_store_statistics.h"

namespace kuzu {
namespace planner {

class Planner {
public:
    static std::unique_ptr<LogicalPlan> getBestPlan(const catalog::Catalog& catalog,
        const storage::NodesStoreStatsAndDeletedIDs& nodesStatistics,
        const storage::RelsStoreStats& relsStatistics, const binder::BoundStatement& statement);

    static std::vector<std::unique_ptr<LogicalPlan>> getAllPlans(const catalog::Catalog& catalog,
        const storage::NodesStoreStatsAndDeletedIDs& nodesStatistics,
        const storage::RelsStoreStats& relsStatistics, const binder::BoundStatement& statement);

private:
    static std::unique_ptr<LogicalPlan> planCreateTable(const binder::BoundStatement& statement);

    static std::unique_ptr<LogicalPlan> planDropTable(const binder::BoundStatement& statement);

    static std::unique_ptr<LogicalPlan> planAlter(const binder::BoundStatement& statement);

    static std::unique_ptr<LogicalPlan> planStandaloneCall(const binder::BoundStatement& statement);

    static std::unique_ptr<LogicalPlan> planCommentOn(const binder::BoundStatement& statement);

    static std::unique_ptr<LogicalPlan> planExplain(const catalog::Catalog& catalog,
        const storage::NodesStoreStatsAndDeletedIDs& nodesStatistics,
        const storage::RelsStoreStats& relsStatistics, const binder::BoundStatement& statement);

    static std::unique_ptr<LogicalPlan> planCreateMacro(const binder::BoundStatement& statement);

    static std::unique_ptr<LogicalPlan> planTransaction(const binder::BoundStatement& statement);

    static std::vector<std::unique_ptr<LogicalPlan>> getAllQueryPlans(
        const catalog::Catalog& catalog,
        const storage::NodesStoreStatsAndDeletedIDs& nodesStatistics,
        const storage::RelsStoreStats& relsStatistics, const binder::BoundStatement& statement);

    static std::vector<std::unique_ptr<LogicalPlan>> getAllExplainPlans(
        const catalog::Catalog& catalog,
        const storage::NodesStoreStatsAndDeletedIDs& nodesStatistics,
        const storage::RelsStoreStats& relsStatistics, const binder::BoundStatement& statement);

    static std::unique_ptr<LogicalPlan> planCopyTo(const catalog::Catalog& catalog,
        const storage::NodesStoreStatsAndDeletedIDs& nodesStatistics,
        const storage::RelsStoreStats& relsStatistics, const binder::BoundStatement& statement);

    static std::unique_ptr<LogicalPlan> planCopyFrom(const binder::BoundStatement& statement);

    static std::unique_ptr<LogicalPlan> getSimplePlan(std::shared_ptr<LogicalOperator> op);
};

} // namespace planner
} // namespace kuzu
