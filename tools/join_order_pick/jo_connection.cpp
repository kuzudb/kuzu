#include "jo_connection.h"

#include "binder/binder.h"
#include "parser/parser.h"
#include "planner/logical_plan/logical_plan_util.h"
#include "planner/planner.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::planner;

namespace kuzu {
namespace main {

std::unique_ptr<QueryResult> JOConnection::query(
    const std::string& query, const std::string& encodedJoin) {
    assert(!query.empty());
    lock_t lck{mtx};
    auto preparedStatement = std::make_unique<PreparedStatement>();
    try {
        auto statement = Parser::parseQuery(query);
        assert(statement->getStatementType() == StatementType::QUERY);
        auto parsedQuery = (RegularQuery*)statement.get();
        auto boundQuery = Binder(*database->catalog).bind(*parsedQuery);
        auto& nodesStatisticsAndDeletedIDs =
            database->storageManager->getNodesStore().getNodesStatisticsAndDeletedIDs();
        auto& relsStatistics = database->storageManager->getRelsStore().getRelsStatistics();
        std::unique_ptr<LogicalPlan> logicalPlan;
        if (encodedJoin.empty()) {
            logicalPlan = Planner::getBestPlan(
                *database->catalog, nodesStatisticsAndDeletedIDs, relsStatistics, *boundQuery);
        } else {
            for (auto& plan : Planner::getAllPlans(*database->catalog, nodesStatisticsAndDeletedIDs,
                     relsStatistics, *boundQuery)) {
                if (LogicalPlanUtil::encodeJoin(*plan) == encodedJoin) {
                    logicalPlan = move(plan);
                }
            }
            if (logicalPlan == nullptr) {
                throw ConnectionException("Cannot find a plan matching " + encodedJoin);
            }
        }
        preparedStatement->statementResult = boundQuery->getStatementResult()->copy();
        preparedStatement->logicalPlans.push_back(std::move(logicalPlan));
        return executeAndAutoCommitIfNecessaryNoLock(preparedStatement.get());
    } catch (Exception& exception) {
        std::string errMsg = exception.what();
        return queryResultWithError(errMsg);
    }
}

} // namespace main
} // namespace kuzu
