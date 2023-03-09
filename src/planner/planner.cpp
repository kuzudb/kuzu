#include "planner/planner.h"

#include "binder/copy/bound_copy.h"
#include "binder/ddl/bound_add_property.h"
#include "binder/ddl/bound_create_node_clause.h"
#include "binder/ddl/bound_create_rel_clause.h"
#include "binder/ddl/bound_drop_property.h"
#include "binder/ddl/bound_drop_table.h"
#include "binder/ddl/bound_rename_property.h"
#include "binder/ddl/bound_rename_table.h"
#include "planner/logical_plan/logical_operator/logical_add_property.h"
#include "planner/logical_plan/logical_operator/logical_copy.h"
#include "planner/logical_plan/logical_operator/logical_create_node_table.h"
#include "planner/logical_plan/logical_operator/logical_create_rel_table.h"
#include "planner/logical_plan/logical_operator/logical_drop_property.h"
#include "planner/logical_plan/logical_operator/logical_drop_table.h"
#include "planner/logical_plan/logical_operator/logical_rename_property.h"
#include "planner/logical_plan/logical_operator/logical_rename_table.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace planner {

std::unique_ptr<LogicalPlan> Planner::getBestPlan(const Catalog& catalog,
    const NodesStatisticsAndDeletedIDs& nodesStatistics, const RelsStatistics& relsStatistics,
    const BoundStatement& statement) {
    std::unique_ptr<LogicalPlan> plan;
    switch (statement.getStatementType()) {
    case StatementType::QUERY: {
        plan = QueryPlanner(catalog, nodesStatistics, relsStatistics).getBestPlan(statement);
    } break;
    case StatementType::CREATE_NODE_CLAUSE: {
        plan = planCreateNodeTable(statement);
    } break;
    case StatementType::CREATE_REL_CLAUSE: {
        plan = planCreateRelTable(statement);
    } break;
    case StatementType::COPY_CSV: {
        plan = planCopy(statement);
    } break;
    case StatementType::DROP_TABLE: {
        plan = planDropTable(statement);
    } break;
    case StatementType::RENAME_TABLE: {
        plan = planRenameTable(statement);
    } break;
    case StatementType::ADD_PROPERTY: {
        plan = planAddProperty(statement);
    } break;
    case StatementType::DROP_PROPERTY: {
        plan = planDropProperty(statement);
    } break;
    case StatementType::RENAME_PROPERTY: {
        plan = planRenameProperty(statement);
    } break;
    default:
        throw common::NotImplementedException("getBestPlan()");
    }
    // Avoid sharing operator across plans.
    return plan->deepCopy();
}

std::vector<std::unique_ptr<LogicalPlan>> Planner::getAllPlans(const Catalog& catalog,
    const NodesStatisticsAndDeletedIDs& nodesStatistics, const RelsStatistics& relsStatistics,
    const BoundStatement& statement) {
    // We enumerate all plans for our testing framework. This API should only be used for QUERY
    // but not DDL or COPY_CSV.
    assert(statement.getStatementType() == StatementType::QUERY);
    auto planner = QueryPlanner(catalog, nodesStatistics, relsStatistics);
    std::vector<std::unique_ptr<LogicalPlan>> plans;
    for (auto& plan : planner.getAllPlans(statement)) {
        // Avoid sharing operator across plans.
        plans.push_back(plan->deepCopy());
    }
    return plans;
}

std::unique_ptr<LogicalPlan> Planner::planCreateNodeTable(const BoundStatement& statement) {
    auto& createNodeClause = (BoundCreateNodeClause&)statement;
    auto plan = std::make_unique<LogicalPlan>();
    auto createNodeTable = make_shared<LogicalCreateNodeTable>(createNodeClause.getTableName(),
        createNodeClause.getPropertyNameDataTypes(), createNodeClause.getPrimaryKeyIdx(),
        statement.getStatementResult()->getSingleExpressionToCollect());
    plan->setLastOperator(std::move(createNodeTable));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planCreateRelTable(const BoundStatement& statement) {
    auto& createRelClause = (BoundCreateRelClause&)statement;
    auto plan = std::make_unique<LogicalPlan>();
    auto createRelTable = make_shared<LogicalCreateRelTable>(createRelClause.getTableName(),
        createRelClause.getPropertyNameDataTypes(), createRelClause.getRelMultiplicity(),
        createRelClause.getSrcTableID(), createRelClause.getDstTableID(),
        statement.getStatementResult()->getSingleExpressionToCollect());
    plan->setLastOperator(std::move(createRelTable));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planDropTable(const BoundStatement& statement) {
    auto& dropTableClause = (BoundDropTable&)statement;
    auto plan = std::make_unique<LogicalPlan>();
    auto dropTable =
        make_shared<LogicalDropTable>(dropTableClause.getTableID(), dropTableClause.getTableName(),
            statement.getStatementResult()->getSingleExpressionToCollect());
    plan->setLastOperator(std::move(dropTable));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planRenameTable(const BoundStatement& statement) {
    auto& renameTableClause = (BoundRenameTable&)statement;
    auto plan = std::make_unique<LogicalPlan>();
    auto renameTable = make_shared<LogicalRenameTable>(renameTableClause.getTableID(),
        renameTableClause.getTableName(), renameTableClause.getNewName(),
        statement.getStatementResult()->getSingleExpressionToCollect());
    plan->setLastOperator(std::move(renameTable));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planAddProperty(const BoundStatement& statement) {
    auto& addPropertyClause = (BoundAddProperty&)statement;
    auto plan = std::make_unique<LogicalPlan>();
    auto addProperty = make_shared<LogicalAddProperty>(addPropertyClause.getTableID(),
        addPropertyClause.getPropertyName(), addPropertyClause.getDataType(),
        addPropertyClause.getDefaultValue(), addPropertyClause.getTableName(),
        statement.getStatementResult()->getSingleExpressionToCollect());
    plan->setLastOperator(std::move(addProperty));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planDropProperty(const BoundStatement& statement) {
    auto& dropPropertyClause = (BoundDropProperty&)statement;
    auto plan = std::make_unique<LogicalPlan>();
    auto dropProperty = make_shared<LogicalDropProperty>(dropPropertyClause.getTableID(),
        dropPropertyClause.getPropertyID(), dropPropertyClause.getTableName(),
        statement.getStatementResult()->getSingleExpressionToCollect());
    plan->setLastOperator(std::move(dropProperty));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planRenameProperty(const BoundStatement& statement) {
    auto& renamePropertyClause = (BoundRenameProperty&)statement;
    auto plan = std::make_unique<LogicalPlan>();
    auto renameProperty = make_shared<LogicalRenameProperty>(renamePropertyClause.getTableID(),
        renamePropertyClause.getTableName(), renamePropertyClause.getPropertyID(),
        renamePropertyClause.getNewName(),
        statement.getStatementResult()->getSingleExpressionToCollect());
    plan->setLastOperator(std::move(renameProperty));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planCopy(const BoundStatement& statement) {
    auto& copyCSVClause = (BoundCopy&)statement;
    auto plan = std::make_unique<LogicalPlan>();
    auto copyCSV = make_shared<LogicalCopy>(copyCSVClause.getCopyDescription(),
        copyCSVClause.getTableID(), copyCSVClause.getTableName());
    plan->setLastOperator(std::move(copyCSV));
    return plan;
}

} // namespace planner
} // namespace kuzu
