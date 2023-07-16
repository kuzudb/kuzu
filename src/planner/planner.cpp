#include "planner/planner.h"

#include "binder/bound_explain.h"
#include "binder/call/bound_standalone_call.h"
#include "binder/copy/bound_copy.h"
#include "binder/ddl/bound_add_property.h"
#include "binder/ddl/bound_create_node_clause.h"
#include "binder/ddl/bound_create_rel_clause.h"
#include "binder/ddl/bound_drop_property.h"
#include "binder/ddl/bound_drop_table.h"
#include "binder/ddl/bound_rename_property.h"
#include "binder/ddl/bound_rename_table.h"
#include "binder/expression/variable_expression.h"
#include "planner/logical_plan/logical_operator/logical_add_property.h"
#include "planner/logical_plan/logical_operator/logical_copy.h"
#include "planner/logical_plan/logical_operator/logical_create_node_table.h"
#include "planner/logical_plan/logical_operator/logical_create_rel_table.h"
#include "planner/logical_plan/logical_operator/logical_drop_property.h"
#include "planner/logical_plan/logical_operator/logical_drop_table.h"
#include "planner/logical_plan/logical_operator/logical_explain.h"
#include "planner/logical_plan/logical_operator/logical_in_query_call.h"
#include "planner/logical_plan/logical_operator/logical_rename_property.h"
#include "planner/logical_plan/logical_operator/logical_rename_table.h"
#include "planner/logical_plan/logical_operator/logical_standalone_call.h"

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
    case StatementType::CREATE_NODE_TABLE: {
        plan = planCreateNodeTable(statement);
    } break;
    case StatementType::CREATE_REL_TABLE: {
        plan = planCreateRelTable(statement);
    } break;
    case StatementType::COPY: {
        plan = planCopy(catalog, statement);
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
    case StatementType::STANDALONE_CALL: {
        plan = planStandaloneCall(statement);
    } break;
    case StatementType::EXPLAIN: {
        plan = planExplain(catalog, nodesStatistics, relsStatistics, statement);
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
    // We enumerate all plans for our testing framework. This API should only be used for QUERY,
    // EXPLAIN, but not DDL or COPY.
    switch (statement.getStatementType()) {
    case StatementType::QUERY:
        return getAllQueryPlans(catalog, nodesStatistics, relsStatistics, statement);
    case StatementType::EXPLAIN:
        return getAllExplainPlans(catalog, nodesStatistics, relsStatistics, statement);
    default:
        throw NotImplementedException("Planner::getAllPlans");
    }
}

std::unique_ptr<LogicalPlan> Planner::planCreateNodeTable(const BoundStatement& statement) {
    auto& createNodeClause = reinterpret_cast<const BoundCreateNodeClause&>(statement);
    auto plan = std::make_unique<LogicalPlan>();
    auto createNodeTable = make_shared<LogicalCreateNodeTable>(createNodeClause.getTableName(),
        createNodeClause.getProperties(), createNodeClause.getPrimaryKeyIdx(),
        statement.getStatementResult()->getSingleExpressionToCollect());
    plan->setLastOperator(std::move(createNodeTable));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planCreateRelTable(const BoundStatement& statement) {
    auto& createRelClause = reinterpret_cast<const BoundCreateRelClause&>(statement);
    auto plan = std::make_unique<LogicalPlan>();
    auto createRelTable = make_shared<LogicalCreateRelTable>(createRelClause.getTableName(),
        createRelClause.getProperties(), createRelClause.getRelMultiplicity(),
        createRelClause.getSrcTableID(), createRelClause.getDstTableID(),
        statement.getStatementResult()->getSingleExpressionToCollect());
    plan->setLastOperator(std::move(createRelTable));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planDropTable(const BoundStatement& statement) {
    auto& dropTableClause = reinterpret_cast<const BoundDropTable&>(statement);
    auto plan = std::make_unique<LogicalPlan>();
    auto dropTable =
        make_shared<LogicalDropTable>(dropTableClause.getTableID(), dropTableClause.getTableName(),
            statement.getStatementResult()->getSingleExpressionToCollect());
    plan->setLastOperator(std::move(dropTable));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planRenameTable(const BoundStatement& statement) {
    auto& renameTableClause = reinterpret_cast<const BoundRenameTable&>(statement);
    auto plan = std::make_unique<LogicalPlan>();
    auto renameTable = make_shared<LogicalRenameTable>(renameTableClause.getTableID(),
        renameTableClause.getTableName(), renameTableClause.getNewName(),
        statement.getStatementResult()->getSingleExpressionToCollect());
    plan->setLastOperator(std::move(renameTable));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planAddProperty(const BoundStatement& statement) {
    auto& addPropertyClause = reinterpret_cast<const BoundAddProperty&>(statement);
    auto plan = std::make_unique<LogicalPlan>();
    auto addProperty = make_shared<LogicalAddProperty>(addPropertyClause.getTableID(),
        addPropertyClause.getPropertyName(), addPropertyClause.getDataType(),
        addPropertyClause.getDefaultValue(), addPropertyClause.getTableName(),
        statement.getStatementResult()->getSingleExpressionToCollect());
    plan->setLastOperator(std::move(addProperty));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planDropProperty(const BoundStatement& statement) {
    auto& dropPropertyClause = reinterpret_cast<const BoundDropProperty&>(statement);
    auto plan = std::make_unique<LogicalPlan>();
    auto dropProperty = make_shared<LogicalDropProperty>(dropPropertyClause.getTableID(),
        dropPropertyClause.getPropertyID(), dropPropertyClause.getTableName(),
        statement.getStatementResult()->getSingleExpressionToCollect());
    plan->setLastOperator(std::move(dropProperty));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planRenameProperty(const BoundStatement& statement) {
    auto& renamePropertyClause = reinterpret_cast<const BoundRenameProperty&>(statement);
    auto plan = std::make_unique<LogicalPlan>();
    auto renameProperty = make_shared<LogicalRenameProperty>(renamePropertyClause.getTableID(),
        renamePropertyClause.getTableName(), renamePropertyClause.getPropertyID(),
        renamePropertyClause.getNewName(),
        statement.getStatementResult()->getSingleExpressionToCollect());
    plan->setLastOperator(std::move(renameProperty));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planCopy(
    const catalog::Catalog& catalog, const BoundStatement& statement) {
    auto& copyClause = reinterpret_cast<const BoundCopy&>(statement);
    auto plan = std::make_unique<LogicalPlan>();
    expression_vector arrowColumnExpressions;
    for (auto& property :
        catalog.getReadOnlyVersion()->getTableSchema(copyClause.getTableID())->properties) {
        if (property.dataType.getLogicalTypeID() != common::LogicalTypeID::SERIAL) {
            arrowColumnExpressions.push_back(std::make_shared<VariableExpression>(
                common::LogicalType{common::LogicalTypeID::ARROW_COLUMN}, property.name,
                property.name));
        }
    }
    auto copy = make_shared<LogicalCopy>(copyClause.getCopyDescription(), copyClause.getTableID(),
        copyClause.getTableName(), std::move(arrowColumnExpressions),
        std::make_shared<VariableExpression>(
            common::LogicalType{common::LogicalTypeID::INT64}, "startOffset", "startOffset"),
        std::make_shared<VariableExpression>(
            common::LogicalType{common::LogicalTypeID::INT64}, "columnIdx", "columnIdx"),
        copyClause.getStatementResult()->getSingleExpressionToCollect());
    plan->setLastOperator(std::move(copy));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planStandaloneCall(const BoundStatement& statement) {
    auto& standaloneCallClause = reinterpret_cast<const BoundStandaloneCall&>(statement);
    auto plan = std::make_unique<LogicalPlan>();
    auto logicalStandaloneCall = make_shared<LogicalStandaloneCall>(
        standaloneCallClause.getOption(), standaloneCallClause.getOptionValue());
    plan->setLastOperator(std::move(logicalStandaloneCall));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planExplain(const Catalog& catalog,
    const NodesStatisticsAndDeletedIDs& nodesStatistics, const RelsStatistics& relsStatistics,
    const BoundStatement& statement) {
    auto& explain = reinterpret_cast<const BoundExplain&>(statement);
    auto statementToExplain = explain.getStatementToExplain();
    auto plan = getBestPlan(catalog, nodesStatistics, relsStatistics, *statementToExplain);
    auto logicalExplain = make_shared<LogicalExplain>(plan->getLastOperator(),
        statement.getStatementResult()->getSingleExpressionToCollect(), explain.getExplainType(),
        explain.getStatementToExplain()->getStatementResult()->getColumns());
    plan->setLastOperator(std::move(logicalExplain));
    return plan;
}

std::vector<std::unique_ptr<LogicalPlan>> Planner::getAllQueryPlans(const catalog::Catalog& catalog,
    const storage::NodesStatisticsAndDeletedIDs& nodesStatistics,
    const storage::RelsStatistics& relsStatistics, const BoundStatement& statement) {
    auto planner = QueryPlanner(catalog, nodesStatistics, relsStatistics);
    std::vector<std::unique_ptr<LogicalPlan>> plans;
    for (auto& plan : planner.getAllPlans(statement)) {
        // Avoid sharing operator across plans.
        plans.push_back(plan->deepCopy());
    }
    return plans;
}

std::vector<std::unique_ptr<LogicalPlan>> Planner::getAllExplainPlans(
    const catalog::Catalog& catalog, const storage::NodesStatisticsAndDeletedIDs& nodesStatistics,
    const storage::RelsStatistics& relsStatistics, const BoundStatement& statement) {
    auto& explainStatement = reinterpret_cast<const BoundExplain&>(statement);
    auto statementToExplain = explainStatement.getStatementToExplain();
    auto plans = getAllPlans(catalog, nodesStatistics, relsStatistics, *statementToExplain);
    for (auto& plan : plans) {
        auto logicalExplain = make_shared<LogicalExplain>(plan->getLastOperator(),
            statement.getStatementResult()->getSingleExpressionToCollect(),
            explainStatement.getExplainType(),
            explainStatement.getStatementToExplain()->getStatementResult()->getColumns());
        plan->setLastOperator(std::move(logicalExplain));
    }
    return plans;
}

} // namespace planner
} // namespace kuzu
