#pragma once

#include "binder/bound_statement.h"
#include "catalog/catalog.h"
#include "planner/operator/logical_plan.h"
#include "storage/storage_manager.h"

namespace kuzu {

namespace planner {

class Planner {
    friend class QueryPlanner;

public:
    Planner(catalog::Catalog* catalog, storage::StorageManager* storageManager)
        : catalog{catalog}, storageManager{storageManager} {}

    std::unique_ptr<LogicalPlan> getBestPlan(const binder::BoundStatement& statement);

    std::vector<std::unique_ptr<LogicalPlan>> getAllPlans(const binder::BoundStatement& statement);

private:
    std::unique_ptr<LogicalPlan> planCreateTable(const binder::BoundStatement& statement);

    std::unique_ptr<LogicalPlan> planDropTable(const binder::BoundStatement& statement);

    std::unique_ptr<LogicalPlan> planAlter(const binder::BoundStatement& statement);

    std::unique_ptr<LogicalPlan> planStandaloneCall(const binder::BoundStatement& statement);

    std::unique_ptr<LogicalPlan> planCommentOn(const binder::BoundStatement& statement);

    std::unique_ptr<LogicalPlan> planExplain(const binder::BoundStatement& statement);

    std::unique_ptr<LogicalPlan> planCreateMacro(const binder::BoundStatement& statement);

    std::unique_ptr<LogicalPlan> planTransaction(const binder::BoundStatement& statement);

    std::vector<std::unique_ptr<LogicalPlan>> getAllQueryPlans(
        const binder::BoundStatement& statement);

    std::vector<std::unique_ptr<LogicalPlan>> getAllExplainPlans(
        const binder::BoundStatement& statement);

    std::unique_ptr<LogicalPlan> planCopyTo(const binder::BoundStatement& statement);

    std::unique_ptr<LogicalPlan> planCopyFrom(const binder::BoundStatement& statement);

    std::unique_ptr<LogicalPlan> getSimplePlan(std::shared_ptr<LogicalOperator> op);

private:
    catalog::Catalog* catalog;
    storage::StorageManager* storageManager;
};

} // namespace planner
} // namespace kuzu
