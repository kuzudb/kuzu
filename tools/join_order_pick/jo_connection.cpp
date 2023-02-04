#include "jo_connection.h"

#include "binder/binder.h"
#include "parser/parser.h"
#include "planner/logical_plan/logical_plan_util.h"
#include "planner/planner.h"
#include "processor/processor.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/storage_manager.h"
#include "transaction/transaction.h"
#include "transaction/transaction_manager.h"

#include <json.hpp>
namespace kuzu {
namespace main {

unique_ptr<QueryResult> JOConnection::query(const string& query, const string& encodedJoin) {
    assert(!query.empty());
    lock_t lck{mtx};
    auto preparedStatement = make_unique<PreparedStatement>();
    try {
        auto statement = Parser::parseQuery(query);
        assert(statement->getStatementType() == StatementType::QUERY);
        auto parsedQuery = (RegularQuery*)statement.get();
        auto boundQuery = Binder(*database->catalog).bind(*parsedQuery);
        auto& nodesStatisticsAndDeletedIDs =
            database->storageManager->getNodesStore().getNodesStatisticsAndDeletedIDs();
        auto& relsStatistics = database->storageManager->getRelsStore().getRelsStatistics();
        unique_ptr<LogicalPlan> logicalPlan;
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
        string errMsg = exception.what();
        return queryResultWithError(errMsg);
    }
}

} // namespace main
} // namespace kuzu
