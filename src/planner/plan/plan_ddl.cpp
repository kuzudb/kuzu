#include "binder/ddl/bound_alter.h"
#include "binder/ddl/bound_create_table.h"
#include "binder/ddl/bound_drop_table.h"
#include "planner/operator/ddl/logical_alter.h"
#include "planner/operator/ddl/logical_create_table.h"
#include "planner/operator/ddl/logical_drop_table.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

std::unique_ptr<LogicalPlan> Planner::getSimplePlan(std::shared_ptr<LogicalOperator> op) {
    auto plan = std::make_unique<LogicalPlan>();
    op->computeFactorizedSchema();
    plan->setLastOperator(std::move(op));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planCreateTable(const BoundStatement& statement) {
    auto& creatTable = reinterpret_cast<const BoundCreateTable&>(statement);
    auto info = creatTable.getInfo();
    auto logicalCreateTable = make_shared<LogicalCreateTable>(info->tableName, info->copy(),
        statement.getStatementResult()->getSingleExpressionToCollect());
    return getSimplePlan(std::move(logicalCreateTable));
}

std::unique_ptr<LogicalPlan> Planner::planDropTable(const BoundStatement& statement) {
    auto& dropTable = reinterpret_cast<const BoundDropTable&>(statement);
    auto logicalDropTable = make_shared<LogicalDropTable>(dropTable.getTableID(),
        dropTable.getTableName(), statement.getStatementResult()->getSingleExpressionToCollect());
    return getSimplePlan(std::move(logicalDropTable));
}

std::unique_ptr<LogicalPlan> Planner::planAlter(const BoundStatement& statement) {
    auto& alter = reinterpret_cast<const BoundAlter&>(statement);
    auto info = alter.getInfo();
    auto logicalAlter = std::make_shared<LogicalAlter>(info->copy(), info->tableName,
        statement.getStatementResult()->getSingleExpressionToCollect());
    return getSimplePlan(std::move(logicalAlter));
}

} // namespace planner
} // namespace kuzu
