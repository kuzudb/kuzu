#include "binder/bound_attach_database.h"
#include "binder/bound_create_macro.h"
#include "binder/bound_detach_database.h"
#include "binder/bound_explain.h"
#include "binder/bound_extension_statement.h"
#include "binder/bound_standalone_call.h"
#include "binder/bound_standalone_call_function.h"
#include "binder/bound_transaction_statement.h"
#include "binder/bound_use_database.h"
#include "binder/ddl/bound_alter.h"
#include "binder/ddl/bound_create_sequence.h"
#include "binder/ddl/bound_create_table.h"
#include "binder/ddl/bound_create_type.h"
#include "binder/ddl/bound_drop.h"
#include "binder/expression/literal_expression.h"
#include "planner/operator/ddl/logical_alter.h"
#include "planner/operator/ddl/logical_create_sequence.h"
#include "planner/operator/ddl/logical_create_table.h"
#include "planner/operator/ddl/logical_create_type.h"
#include "planner/operator/ddl/logical_drop.h"
#include "planner/operator/logical_create_macro.h"
#include "planner/operator/logical_dummy_sink.h"
#include "planner/operator/logical_explain.h"
#include "planner/operator/logical_noop.h"
#include "planner/operator/logical_standalone_call.h"
#include "planner/operator/logical_table_function_call.h"
#include "planner/operator/logical_transaction.h"
#include "planner/operator/simple/logical_attach_database.h"
#include "planner/operator/simple/logical_detach_database.h"
#include "planner/operator/simple/logical_extension.h"
#include "planner/operator/simple/logical_use_database.h"
#include "planner/planner.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

static LogicalPlan getSimplePlan(std::shared_ptr<LogicalOperator> op) {
    LogicalPlan plan;
    op->computeFactorizedSchema();
    plan.setLastOperator(std::move(op));
    return plan;
}

LogicalPlan Planner::planCreateTable(const BoundStatement& statement) {
    auto& createTable = statement.constCast<BoundCreateTable>();
    auto& info = createTable.getInfo();

    // If it is a CREATE NODE TABLE AS, then copy as well
    if (createTable.hasCopyInfo()) {
        // TODO(Xiyang): we should get rid of this dummyStr if CREATE TABLE AS has actual output.
        // Currently, we need dummyStr as placeholder for the createTable & copy pipeline output
        // messages. even though we do NOT actually output them
        auto dummyStr = std::make_shared<LiteralExpression>(Value("dummy"), "dummy");
        std::vector<std::shared_ptr<LogicalOperator>> children;
        auto copyPlan = planCopyNodeFrom(&createTable.getCopyInfo(), {dummyStr});
        children.push_back(copyPlan.getLastOperator());
        auto create = std::make_shared<LogicalCreateTable>(info.copy(), dummyStr);
        auto dummySink = std::make_shared<LogicalDummySink>(std::move(create));
        children.push_back(std::move(dummySink));
        auto noop = std::make_shared<LogicalNoop>(children);
        return getSimplePlan(std::move(noop));
    }
    auto op = std::make_shared<LogicalCreateTable>(info.copy(), statement.getSingleColumnExpr());
    return getSimplePlan(std::move(op));
}

LogicalPlan Planner::planCreateType(const BoundStatement& statement) {
    auto& createType = statement.constCast<BoundCreateType>();
    auto op = std::make_shared<LogicalCreateType>(createType.getName(), createType.getType().copy(),
        statement.getSingleColumnExpr());
    return getSimplePlan(std::move(op));
}

LogicalPlan Planner::planCreateSequence(const BoundStatement& statement) {
    auto& createSequence = statement.constCast<BoundCreateSequence>();
    auto& info = createSequence.getInfo();
    auto op = std::make_shared<LogicalCreateSequence>(info.copy(), statement.getSingleColumnExpr());
    return getSimplePlan(std::move(op));
}

LogicalPlan Planner::planDrop(const BoundStatement& statement) {
    auto& dropTable = statement.constCast<BoundDrop>();
    auto op =
        std::make_shared<LogicalDrop>(dropTable.getDropInfo(), statement.getSingleColumnExpr());
    return getSimplePlan(std::move(op));
}

LogicalPlan Planner::planAlter(const BoundStatement& statement) {
    auto& alter = statement.constCast<BoundAlter>();
    auto op =
        std::make_shared<LogicalAlter>(alter.getInfo().copy(), statement.getSingleColumnExpr());
    return getSimplePlan(std::move(op));
}

LogicalPlan Planner::planStandaloneCall(const BoundStatement& statement) {
    auto& standaloneCallClause = statement.constCast<BoundStandaloneCall>();
    auto op = std::make_shared<LogicalStandaloneCall>(standaloneCallClause.getOption(),
        standaloneCallClause.getOptionValue());
    return getSimplePlan(std::move(op));
}

LogicalPlan Planner::planStandaloneCallFunction(const BoundStatement& statement) {
    auto& standaloneCallFunctionClause = statement.constCast<BoundStandaloneCallFunction>();
    auto op =
        std::make_shared<LogicalTableFunctionCall>(standaloneCallFunctionClause.getTableFunction(),
            standaloneCallFunctionClause.getBindData()->copy());
    return getSimplePlan(std::move(op));
}

LogicalPlan Planner::planExplain(const BoundStatement& statement) {
    auto& explain = statement.constCast<BoundExplain>();
    auto statementToExplain = explain.getStatementToExplain();
    auto planToExplain = planStatement(*statementToExplain);
    auto op = std::make_shared<LogicalExplain>(planToExplain.getLastOperator(),
        statement.getSingleColumnExpr(), explain.getExplainType(),
        statementToExplain->getStatementResult()->getColumns());
    return getSimplePlan(std::move(op));
}

LogicalPlan Planner::planCreateMacro(const BoundStatement& statement) {
    auto& createMacro = statement.constCast<BoundCreateMacro>();
    auto op = std::make_shared<LogicalCreateMacro>(statement.getSingleColumnExpr(),
        createMacro.getMacroName(), createMacro.getMacro());
    return getSimplePlan(std::move(op));
}

LogicalPlan Planner::planTransaction(const BoundStatement& statement) {
    auto& transactionStatement = statement.constCast<BoundTransactionStatement>();
    auto op = std::make_shared<LogicalTransaction>(transactionStatement.getTransactionAction());
    return getSimplePlan(std::move(op));
}

LogicalPlan Planner::planExtension(const BoundStatement& statement) {
    auto& extensionStatement = statement.constCast<BoundExtensionStatement>();
    auto op = std::make_shared<LogicalExtension>(extensionStatement.getAuxInfo(),
        statement.getSingleColumnExpr());
    return getSimplePlan(std::move(op));
}

LogicalPlan Planner::planAttachDatabase(const BoundStatement& statement) {
    auto& boundAttachDatabase = statement.constCast<BoundAttachDatabase>();
    auto op = std::make_shared<LogicalAttachDatabase>(boundAttachDatabase.getAttachInfo(),
        statement.getSingleColumnExpr());
    return getSimplePlan(std::move(op));
}

LogicalPlan Planner::planDetachDatabase(const BoundStatement& statement) {
    auto& boundDetachDatabase = statement.constCast<BoundDetachDatabase>();
    auto op = std::make_shared<LogicalDetachDatabase>(boundDetachDatabase.getDBName(),
        statement.getSingleColumnExpr());
    return getSimplePlan(std::move(op));
}

LogicalPlan Planner::planUseDatabase(const BoundStatement& statement) {
    auto& boundUseDatabase = statement.constCast<BoundUseDatabase>();
    auto op = std::make_shared<LogicalUseDatabase>(boundUseDatabase.getDBName(),
        statement.getSingleColumnExpr());
    return getSimplePlan(std::move(op));
}

} // namespace planner
} // namespace kuzu
