#include "planner/planner.h"

#include "binder/bound_explain.h"
#include "storage/storage_manager.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace planner {

Planner::Planner(main::ClientContext* clientContext) : clientContext{clientContext} {
    auto nStats = clientContext->getStorageManager()->getNodesStatisticsAndDeletedIDs();
    auto rStats = clientContext->getStorageManager()->getRelsStatistics();
    cardinalityEstimator = CardinalityEstimator(clientContext, nStats, rStats);
    context = JoinOrderEnumeratorContext();
}

std::unique_ptr<LogicalPlan> Planner::getBestPlan(const BoundStatement& statement) {
    auto plan = std::make_unique<LogicalPlan>();
    switch (statement.getStatementType()) {
    case StatementType::QUERY: {
        plan = getBestPlan(planQuery(statement));
    } break;
    case StatementType::CREATE_TABLE: {
        appendCreateTable(statement, *plan);
    } break;
    case StatementType::CREATE_SEQUENCE: {
        appendCreateSequence(statement, *plan);
    } break;
    case StatementType::COPY_FROM: {
        plan = planCopyFrom(statement);
    } break;
    case StatementType::COPY_TO: {
        plan = planCopyTo(statement);
    } break;
    case StatementType::DROP_TABLE: {
        appendDropTable(statement, *plan);
    } break;
    case StatementType::DROP_SEQUENCE: {
        appendDropSequence(statement, *plan);
    } break;
    case StatementType::ALTER: {
        appendAlter(statement, *plan);
    } break;
    case StatementType::STANDALONE_CALL: {
        appendStandaloneCall(statement, *plan);
    } break;
    case StatementType::COMMENT_ON: {
        appendCommentOn(statement, *plan);
    } break;
    case StatementType::EXPLAIN: {
        appendExplain(statement, *plan);
    } break;
    case StatementType::CREATE_MACRO: {
        appendCreateMacro(statement, *plan);
    } break;
    case StatementType::TRANSACTION: {
        appendTransaction(statement, *plan);
    } break;
    case StatementType::EXTENSION: {
        appendExtension(statement, *plan);
    } break;
    case StatementType::EXPORT_DATABASE: {
        plan = planExportDatabase(statement);
    } break;
    case StatementType::IMPORT_DATABASE: {
        plan = planImportDatabase(statement);
    } break;
    case StatementType::ATTACH_DATABASE: {
        appendAttachDatabase(statement, *plan);
    } break;
    case StatementType::DETACH_DATABASE: {
        appendDetachDatabase(statement, *plan);
    } break;
    case StatementType::USE_DATABASE: {
        appendUseDatabase(statement, *plan);
    } break;
    default:
        KU_UNREACHABLE;
    }
    // Avoid sharing operator across plans.
    return plan->deepCopy();
}

std::vector<std::unique_ptr<LogicalPlan>> Planner::getAllPlans(const BoundStatement& statement) {
    // We enumerate all plans for our testing framework. This API should only be used for QUERY,
    // EXPLAIN, but not DDL or COPY.
    std::vector<std::unique_ptr<LogicalPlan>> plans;
    switch (statement.getStatementType()) {
    case StatementType::QUERY: {
        for (auto& plan : planQuery(statement)) {
            // Avoid sharing operator across plans.
            plans.push_back(plan->deepCopy());
        }
    } break;
    case StatementType::EXPLAIN: {
        auto& explain = ku_dynamic_cast<const BoundStatement&, const BoundExplain&>(statement);
        plans = getAllPlans(*explain.getStatementToExplain());
        for (auto& plan : plans) {
            appendExplain(explain, *plan);
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
    return plans;
}

} // namespace planner
} // namespace kuzu
