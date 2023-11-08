#include "planner/planner.h"

#include "binder/bound_comment_on.h"
#include "binder/bound_create_macro.h"
#include "binder/bound_explain.h"
#include "binder/bound_standalone_call.h"
#include "planner/operator/logical_comment_on.h"
#include "planner/operator/logical_create_macro.h"
#include "planner/operator/logical_explain.h"
#include "planner/operator/logical_standalone_call.h"
#include "planner/query_planner.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace planner {

std::unique_ptr<LogicalPlan> Planner::getBestPlan(const Catalog& catalog,
    const NodesStoreStatsAndDeletedIDs& nodesStatistics, const RelsStoreStats& relsStatistics,
    const BoundStatement& statement) {
    std::unique_ptr<LogicalPlan> plan;
    switch (statement.getStatementType()) {
    case StatementType::QUERY: {
        plan = QueryPlanner(catalog, nodesStatistics, relsStatistics).getBestPlan(statement);
    } break;
    case StatementType::CREATE_TABLE: {
        plan = planCreateTable(statement);
    } break;
    case StatementType::COPY_FROM: {
        plan = planCopyFrom(statement);
    } break;
    case StatementType::COPY_TO: {
        plan = planCopyTo(catalog, nodesStatistics, relsStatistics, statement);
    } break;
    case StatementType::DROP_TABLE: {
        plan = planDropTable(statement);
    } break;
    case StatementType::ALTER: {
        plan = planAlter(statement);
    } break;
    case StatementType::STANDALONE_CALL: {
        plan = planStandaloneCall(statement);
    } break;
    case StatementType::COMMENT_ON: {
        plan = planCommentOn(statement);
    } break;
    case StatementType::EXPLAIN: {
        plan = planExplain(catalog, nodesStatistics, relsStatistics, statement);
    } break;
    case StatementType::CREATE_MACRO: {
        plan = planCreateMacro(statement);
    } break;
    case StatementType::TRANSACTION: {
        plan = planTransaction(statement);
    } break;
    default:
        KU_UNREACHABLE;
    }
    // Avoid sharing operator across plans.
    return plan->deepCopy();
}

std::vector<std::unique_ptr<LogicalPlan>> Planner::getAllPlans(const Catalog& catalog,
    const NodesStoreStatsAndDeletedIDs& nodesStatistics, const RelsStoreStats& relsStatistics,
    const BoundStatement& statement) {
    // We enumerate all plans for our testing framework. This API should only be used for QUERY,
    // EXPLAIN, but not DDL or COPY.
    switch (statement.getStatementType()) {
    case StatementType::QUERY:
        return getAllQueryPlans(catalog, nodesStatistics, relsStatistics, statement);
    case StatementType::EXPLAIN:
        return getAllExplainPlans(catalog, nodesStatistics, relsStatistics, statement);
    default:
        KU_UNREACHABLE;
    }
}

std::unique_ptr<LogicalPlan> Planner::planStandaloneCall(const BoundStatement& statement) {
    auto& standaloneCallClause = reinterpret_cast<const BoundStandaloneCall&>(statement);
    auto plan = std::make_unique<LogicalPlan>();
    auto logicalStandaloneCall = make_shared<LogicalStandaloneCall>(
        standaloneCallClause.getOption(), standaloneCallClause.getOptionValue());
    plan->setLastOperator(std::move(logicalStandaloneCall));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planCommentOn(const BoundStatement& statement) {
    auto& commentOnClause = reinterpret_cast<const BoundCommentOn&>(statement);
    auto plan = std::make_unique<LogicalPlan>();
    auto logicalCommentOn = make_shared<LogicalCommentOn>(
        statement.getStatementResult()->getSingleExpressionToCollect(),
        commentOnClause.getTableID(), commentOnClause.getTableName(), commentOnClause.getComment());
    plan->setLastOperator(std::move(logicalCommentOn));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planExplain(const Catalog& catalog,
    const NodesStoreStatsAndDeletedIDs& nodesStatistics, const RelsStoreStats& relsStatistics,
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

std::unique_ptr<LogicalPlan> Planner::planCreateMacro(const BoundStatement& statement) {
    auto& createMacro = reinterpret_cast<const BoundCreateMacro&>(statement);
    auto plan = std::make_unique<LogicalPlan>();
    auto logicalCreateMacro = make_shared<LogicalCreateMacro>(
        statement.getStatementResult()->getSingleExpressionToCollect(), createMacro.getMacroName(),
        createMacro.getMacro());
    plan->setLastOperator(std::move(logicalCreateMacro));
    return plan;
}

std::vector<std::unique_ptr<LogicalPlan>> Planner::getAllQueryPlans(const catalog::Catalog& catalog,
    const storage::NodesStoreStatsAndDeletedIDs& nodesStatistics,
    const storage::RelsStoreStats& relsStatistics, const BoundStatement& statement) {
    auto planner = QueryPlanner(catalog, nodesStatistics, relsStatistics);
    std::vector<std::unique_ptr<LogicalPlan>> plans;
    for (auto& plan : planner.getAllPlans(statement)) {
        // Avoid sharing operator across plans.
        plans.push_back(plan->deepCopy());
    }
    return plans;
}

std::vector<std::unique_ptr<LogicalPlan>> Planner::getAllExplainPlans(
    const catalog::Catalog& catalog, const storage::NodesStoreStatsAndDeletedIDs& nodesStatistics,
    const storage::RelsStoreStats& relsStatistics, const BoundStatement& statement) {
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
