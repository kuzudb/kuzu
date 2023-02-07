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
    switch (statement.getStatementType()) {
    case StatementType::QUERY: {
        return QueryPlanner(catalog, nodesStatistics, relsStatistics).getBestPlan(statement);
    }
    case StatementType::CREATE_NODE_CLAUSE: {
        return planCreateNodeTable(statement);
    }
    case StatementType::CREATE_REL_CLAUSE: {
        return planCreateRelTable(statement);
    }
    case StatementType::COPY_CSV: {
        return planCopy(statement);
    }
    case StatementType::DROP_TABLE: {
        return planDropTable(statement);
    }
    case StatementType::RENAME_TABLE: {
        return planRenameTable(statement);
    }
    case StatementType::ADD_PROPERTY: {
        return planAddProperty(statement);
    }
    case StatementType::DROP_PROPERTY: {
        return planDropProperty(statement);
    }
    case StatementType::RENAME_PROPERTY: {
        return planRenameProperty(statement);
    }
    default:
        assert(false);
    }
}

std::vector<std::unique_ptr<LogicalPlan>> Planner::getAllPlans(const Catalog& catalog,
    const NodesStatisticsAndDeletedIDs& nodesStatistics, const RelsStatistics& relsStatistics,
    const BoundStatement& statement) {
    // We enumerate all plans for our testing framework. This API should only be used for QUERY
    // but not DDL or COPY_CSV.
    assert(statement.getStatementType() == StatementType::QUERY);
    return QueryPlanner(catalog, nodesStatistics, relsStatistics).getAllPlans(statement);
}

std::unique_ptr<LogicalPlan> Planner::planCreateNodeTable(const BoundStatement& statement) {
    auto& createNodeClause = (BoundCreateNodeClause&)statement;
    auto plan = std::make_unique<LogicalPlan>();
    auto createNodeTable = make_shared<LogicalCreateNodeTable>(createNodeClause.getTableName(),
        createNodeClause.getPropertyNameDataTypes(), createNodeClause.getPrimaryKeyIdx(),
        statement.getStatementResult()->getSingleExpressionToCollect());
    createNodeTable->computeSchema();
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
    createRelTable->computeSchema();
    plan->setLastOperator(std::move(createRelTable));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planDropTable(const BoundStatement& statement) {
    auto& dropTableClause = (BoundDropTable&)statement;
    auto plan = std::make_unique<LogicalPlan>();
    auto dropTable =
        make_shared<LogicalDropTable>(dropTableClause.getTableID(), dropTableClause.getTableName(),
            statement.getStatementResult()->getSingleExpressionToCollect());
    dropTable->computeSchema();
    plan->setLastOperator(std::move(dropTable));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planRenameTable(const BoundStatement& statement) {
    auto& renameTableClause = (BoundRenameTable&)statement;
    auto plan = std::make_unique<LogicalPlan>();
    auto renameTable = make_shared<LogicalRenameTable>(renameTableClause.getTableID(),
        renameTableClause.getTableName(), renameTableClause.getNewName(),
        statement.getStatementResult()->getSingleExpressionToCollect());
    renameTable->computeSchema();
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
    addProperty->computeSchema();
    plan->setLastOperator(std::move(addProperty));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planDropProperty(const BoundStatement& statement) {
    auto& dropPropertyClause = (BoundDropProperty&)statement;
    auto plan = std::make_unique<LogicalPlan>();
    auto dropProperty = make_shared<LogicalDropProperty>(dropPropertyClause.getTableID(),
        dropPropertyClause.getPropertyID(), dropPropertyClause.getTableName(),
        statement.getStatementResult()->getSingleExpressionToCollect());
    dropProperty->computeSchema();
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
    renameProperty->computeSchema();
    plan->setLastOperator(std::move(renameProperty));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planCopy(const BoundStatement& statement) {
    auto& copyCSVClause = (BoundCopy&)statement;
    auto plan = std::make_unique<LogicalPlan>();
    auto copyCSV = make_shared<LogicalCopy>(copyCSVClause.getCopyDescription(),
        copyCSVClause.getTableID(), copyCSVClause.getTableName());
    copyCSV->computeSchema();
    plan->setLastOperator(std::move(copyCSV));
    return plan;
}

} // namespace planner
} // namespace kuzu
