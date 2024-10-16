#include "binder/bound_attach_database.h"
#include "binder/bound_create_macro.h"
#include "binder/bound_detach_database.h"
#include "binder/bound_explain.h"
#include "binder/bound_extension_statement.h"
#include "binder/bound_standalone_call.h"
#include "binder/bound_transaction_statement.h"
#include "binder/bound_use_database.h"
#include "binder/ddl/bound_alter.h"
#include "binder/ddl/bound_create_sequence.h"
#include "binder/ddl/bound_create_table.h"
#include "binder/ddl/bound_create_type.h"
#include "binder/ddl/bound_drop.h"
#include "planner/operator/ddl/logical_alter.h"
#include "planner/operator/ddl/logical_create_sequence.h"
#include "planner/operator/ddl/logical_create_table.h"
#include "planner/operator/ddl/logical_create_type.h"
#include "planner/operator/ddl/logical_drop.h"
#include "planner/operator/logical_create_macro.h"
#include "planner/operator/logical_explain.h"
#include "planner/operator/logical_standalone_call.h"
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

void Planner::appendCreateTable(const BoundStatement& statement, LogicalPlan& plan) {
    auto& createTable = statement.constCast<BoundCreateTable>();
    auto info = createTable.getInfo();
    auto printInfo =
        std::make_unique<LogicalCreateTablePrintInfo>(info->type, info->tableName, info->copy());
    auto op = make_shared<LogicalCreateTable>(info->tableName, info->copy(),
        statement.getStatementResult()->getSingleColumnExpr(), std::move(printInfo));
    plan.setLastOperator(std::move(op));
}

void Planner::appendCreateType(const BoundStatement& statement, LogicalPlan& plan) {
    auto& createType = statement.constCast<BoundCreateType>();
    auto printInfo = std::make_unique<LogicalCreateTypePrintInfo>(createType.getName(),
        createType.getType().toString());
    auto op = make_shared<LogicalCreateType>(createType.getName(), createType.getType().copy(),
        statement.getStatementResult()->getSingleColumnExpr(), std::move(printInfo));
    plan.setLastOperator(std::move(op));
}

void Planner::appendCreateSequence(const BoundStatement& statement, LogicalPlan& plan) {
    auto& createSequence = statement.constCast<BoundCreateSequence>();
    auto info = createSequence.getInfo();
    auto printInfo = std::make_unique<LogicalCreateSequencePrintInfo>(info->sequenceName);
    auto op = make_shared<LogicalCreateSequence>(info->sequenceName, info->copy(),
        statement.getStatementResult()->getSingleColumnExpr(), std::move(printInfo));
    plan.setLastOperator(std::move(op));
}

void Planner::appendDrop(const BoundStatement& statement, LogicalPlan& plan) {
    auto& dropTable = statement.constCast<BoundDrop>();
    auto printInfo = std::make_unique<OPPrintInfo>();
    auto op = make_shared<LogicalDrop>(dropTable.getDropInfo(),
        statement.getStatementResult()->getSingleColumnExpr(), std::move(printInfo));
    plan.setLastOperator(std::move(op));
}

void Planner::appendAlter(const BoundStatement& statement, LogicalPlan& plan) {
    auto& alter = statement.constCast<BoundAlter>();
    auto info = alter.getInfo();
    auto printInfo =
        std::make_unique<LogicalAlterPrintInfo>(info->alterType, info->tableName, info->copy());
    auto op = std::make_shared<LogicalAlter>(info->copy(), info->tableName,
        statement.getStatementResult()->getSingleColumnExpr(), std::move(printInfo));
    plan.setLastOperator(std::move(op));
}

void Planner::appendStandaloneCall(const BoundStatement& statement, LogicalPlan& plan) {
    auto& standaloneCallClause = statement.constCast<BoundStandaloneCall>();
    auto printInfo = std::make_unique<OPPrintInfo>();
    auto op = make_shared<LogicalStandaloneCall>(standaloneCallClause.getOption(),
        standaloneCallClause.getOptionValue(), std::move(printInfo));
    plan.setLastOperator(std::move(op));
}

void Planner::appendExplain(const BoundStatement& statement, LogicalPlan& plan) {
    auto& explain = statement.constCast<BoundExplain>();
    auto statementToExplain = explain.getStatementToExplain();
    auto planToExplain = getBestPlan(*statementToExplain);
    auto printInfo = std::make_unique<OPPrintInfo>();
    auto op = make_shared<LogicalExplain>(planToExplain->getLastOperator(),
        statement.getStatementResult()->getSingleColumnExpr(), explain.getExplainType(),
        explain.getStatementToExplain()->getStatementResult()->getColumns(), std::move(printInfo));
    plan.setLastOperator(std::move(op));
}

void Planner::appendCreateMacro(const BoundStatement& statement, LogicalPlan& plan) {
    auto& createMacro = statement.constCast<BoundCreateMacro>();
    auto printInfo = std::make_unique<LogicalCreateMacroPrintInfo>(createMacro.getMacroName());
    auto op = make_shared<LogicalCreateMacro>(statement.getStatementResult()->getSingleColumnExpr(),
        createMacro.getMacroName(), createMacro.getMacro(), std::move(printInfo));
    plan.setLastOperator(std::move(op));
}

void Planner::appendTransaction(const BoundStatement& statement, LogicalPlan& plan) {
    auto& transactionStatement = statement.constCast<BoundTransactionStatement>();
    auto printInfo = std::make_unique<OPPrintInfo>();
    auto op = std::make_shared<LogicalTransaction>(transactionStatement.getTransactionAction(),
        std::move(printInfo));
    plan.setLastOperator(std::move(op));
}

void Planner::appendExtension(const BoundStatement& statement, LogicalPlan& plan) {
    auto& extensionStatement = statement.constCast<BoundExtensionStatement>();
    auto printInfo = std::make_unique<OPPrintInfo>();
    auto op = std::make_shared<LogicalExtension>(extensionStatement.getAction(),
        extensionStatement.getPath(), statement.getStatementResult()->getSingleColumnExpr(),
        std::move(printInfo));
    plan.setLastOperator(std::move(op));
}

void Planner::appendAttachDatabase(const BoundStatement& statement, LogicalPlan& plan) {
    auto& boundAttachDatabase = statement.constCast<BoundAttachDatabase>();
    auto printInfo = std::make_unique<LogicalAttachDatabasePrintInfo>(
        boundAttachDatabase.getAttachInfo().dbAlias);
    auto attachDatabase =
        std::make_shared<LogicalAttachDatabase>(boundAttachDatabase.getAttachInfo(),
            statement.getStatementResult()->getSingleColumnExpr(), std::move(printInfo));
    plan.setLastOperator(std::move(attachDatabase));
}

void Planner::appendDetachDatabase(const BoundStatement& statement, LogicalPlan& plan) {
    auto& boundDetachDatabase = statement.constCast<BoundDetachDatabase>();
    auto printInfo = std::make_unique<OPPrintInfo>();
    auto detachDatabase = std::make_shared<LogicalDetachDatabase>(boundDetachDatabase.getDBName(),
        statement.getStatementResult()->getSingleColumnExpr(), std::move(printInfo));
    plan.setLastOperator(std::move(detachDatabase));
}

void Planner::appendUseDatabase(const BoundStatement& statement, LogicalPlan& plan) {
    auto& boundUseDatabase = statement.constCast<BoundUseDatabase>();
    auto printInfo = std::make_unique<OPPrintInfo>();
    auto useDatabase = std::make_shared<LogicalUseDatabase>(boundUseDatabase.getDBName(),
        statement.getStatementResult()->getSingleColumnExpr(), std::move(printInfo));
    plan.setLastOperator(std::move(useDatabase));
}

} // namespace planner
} // namespace kuzu
