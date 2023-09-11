#include "binder/ddl/bound_add_property.h"
#include "binder/ddl/bound_create_table.h"
#include "binder/ddl/bound_drop_property.h"
#include "binder/ddl/bound_drop_table.h"
#include "binder/ddl/bound_rename_property.h"
#include "binder/ddl/bound_rename_table.h"
#include "planner/logical_plan/ddl/logical_add_property.h"
#include "planner/logical_plan/ddl/logical_create_table.h"
#include "planner/logical_plan/ddl/logical_drop_property.h"
#include "planner/logical_plan/ddl/logical_drop_table.h"
#include "planner/logical_plan/ddl/logical_rename_property.h"
#include "planner/logical_plan/ddl/logical_rename_table.h"
#include "planner/planner.h"

namespace kuzu {
namespace planner {

std::unique_ptr<LogicalPlan> Planner::getSimplePlan(std::shared_ptr<LogicalOperator> op) {
    auto plan = std::make_unique<LogicalPlan>();
    op->computeFactorizedSchema();
    plan->setLastOperator(std::move(op));
    return plan;
}

std::unique_ptr<LogicalPlan> Planner::planCreateTable(const BoundStatement& statement) {
    auto& creatTable = (const BoundCreateTable&)(statement);
    auto logicalCreateTable = make_shared<LogicalCreateTable>(creatTable.getTableName(),
        creatTable.getBoundCreateTableInfo()->copy(),
        statement.getStatementResult()->getSingleExpressionToCollect());
    return getSimplePlan(std::move(logicalCreateTable));
}

std::unique_ptr<LogicalPlan> Planner::planDropTable(const BoundStatement& statement) {
    auto& dropTable = reinterpret_cast<const BoundDropTable&>(statement);
    auto logicalDropTable = make_shared<LogicalDropTable>(dropTable.getTableID(),
        dropTable.getTableName(), statement.getStatementResult()->getSingleExpressionToCollect());
    return getSimplePlan(std::move(logicalDropTable));
}

std::unique_ptr<LogicalPlan> Planner::planRenameTable(const BoundStatement& statement) {
    auto& renameTable = reinterpret_cast<const BoundRenameTable&>(statement);
    auto logicalRenameTable = make_shared<LogicalRenameTable>(renameTable.getTableID(),
        renameTable.getTableName(), renameTable.getNewName(),
        statement.getStatementResult()->getSingleExpressionToCollect());
    return getSimplePlan(std::move(logicalRenameTable));
}

std::unique_ptr<LogicalPlan> Planner::planAddProperty(const BoundStatement& statement) {
    auto& addProperty = reinterpret_cast<const BoundAddProperty&>(statement);
    auto logicalAddProperty = make_shared<LogicalAddProperty>(addProperty.getTableID(),
        addProperty.getPropertyName(), addProperty.getDataType()->copy(),
        addProperty.getDefaultValue(), addProperty.getTableName(),
        statement.getStatementResult()->getSingleExpressionToCollect());
    return getSimplePlan(std::move(logicalAddProperty));
}

std::unique_ptr<LogicalPlan> Planner::planDropProperty(const BoundStatement& statement) {
    auto& dropProperty = reinterpret_cast<const BoundDropProperty&>(statement);
    auto logicalDropProperty = make_shared<LogicalDropProperty>(dropProperty.getTableID(),
        dropProperty.getPropertyID(), dropProperty.getTableName(),
        statement.getStatementResult()->getSingleExpressionToCollect());
    return getSimplePlan(logicalDropProperty);
}

std::unique_ptr<LogicalPlan> Planner::planRenameProperty(const BoundStatement& statement) {
    auto& renameProperty = reinterpret_cast<const BoundRenameProperty&>(statement);
    auto logicalRenameProperty = make_shared<LogicalRenameProperty>(renameProperty.getTableID(),
        renameProperty.getTableName(), renameProperty.getPropertyID(), renameProperty.getNewName(),
        statement.getStatementResult()->getSingleExpressionToCollect());
    return getSimplePlan(std::move(logicalRenameProperty));
}

} // namespace planner
} // namespace kuzu
