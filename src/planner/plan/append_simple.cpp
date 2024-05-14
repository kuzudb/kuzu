#include "binder/bound_attach_database.h"
#include "binder/bound_comment_on.h"
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
#include "binder/ddl/bound_drop_sequence.h"
#include "binder/ddl/bound_drop_table.h"
#include "common/cast.h"
#include "planner/operator/ddl/logical_alter.h"
#include "planner/operator/ddl/logical_create_sequence.h"
#include "planner/operator/ddl/logical_create_table.h"
#include "planner/operator/ddl/logical_drop_sequence.h"
#include "planner/operator/ddl/logical_drop_table.h"
#include "planner/operator/logical_comment_on.h"
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
    auto& createTable = ku_dynamic_cast<const BoundStatement&, const BoundCreateTable&>(statement);
    auto info = createTable.getInfo();
    auto op = make_shared<LogicalCreateTable>(info->tableName, info->copy(),
        statement.getStatementResult()->getSingleColumnExpr());
    plan.setLastOperator(std::move(op));
}

void Planner::appendCreateSequence(const BoundStatement& statement, LogicalPlan& plan) {
    auto& createSequence =
        ku_dynamic_cast<const BoundStatement&, const BoundCreateSequence&>(statement);
    auto info = createSequence.getInfo();
    auto op = make_shared<LogicalCreateSequence>(info->sequenceName, info->copy(),
        statement.getStatementResult()->getSingleColumnExpr());
    plan.setLastOperator(std::move(op));
}

void Planner::appendDropTable(const BoundStatement& statement, LogicalPlan& plan) {
    auto& dropTable = ku_dynamic_cast<const BoundStatement&, const BoundDropTable&>(statement);
    auto op = make_shared<LogicalDropTable>(dropTable.getTableID(), dropTable.getTableName(),
        statement.getStatementResult()->getSingleColumnExpr());
    plan.setLastOperator(std::move(op));
}

void Planner::appendDropSequence(const BoundStatement& statement, LogicalPlan& plan) {
    auto& dropSequence =
        ku_dynamic_cast<const BoundStatement&, const BoundDropSequence&>(statement);
    auto op = make_shared<LogicalDropSequence>(dropSequence.getSequenceID(),
        dropSequence.getSequenceName(), statement.getStatementResult()->getSingleColumnExpr());
    plan.setLastOperator(std::move(op));
}

void Planner::appendAlter(const BoundStatement& statement, LogicalPlan& plan) {
    auto& alter = ku_dynamic_cast<const BoundStatement&, const BoundAlter&>(statement);
    auto info = alter.getInfo();
    auto op = std::make_shared<LogicalAlter>(info->copy(), info->tableName,
        statement.getStatementResult()->getSingleColumnExpr());
    plan.setLastOperator(std::move(op));
}

void Planner::appendStandaloneCall(const BoundStatement& statement, LogicalPlan& plan) {
    auto& standaloneCallClause =
        ku_dynamic_cast<const BoundStatement&, const BoundStandaloneCall&>(statement);
    auto op = make_shared<LogicalStandaloneCall>(standaloneCallClause.getOption(),
        standaloneCallClause.getOptionValue());
    plan.setLastOperator(std::move(op));
}

void Planner::appendCommentOn(const BoundStatement& statement, LogicalPlan& plan) {
    auto& commentOnClause =
        ku_dynamic_cast<const BoundStatement&, const BoundCommentOn&>(statement);
    auto op = make_shared<LogicalCommentOn>(statement.getStatementResult()->getSingleColumnExpr(),
        commentOnClause.getTableID(), commentOnClause.getTableName(), commentOnClause.getComment());
    plan.setLastOperator(std::move(op));
}

void Planner::appendExplain(const BoundStatement& statement, LogicalPlan& plan) {
    auto& explain = ku_dynamic_cast<const BoundStatement&, const BoundExplain&>(statement);
    auto statementToExplain = explain.getStatementToExplain();
    auto planToExplain = getBestPlan(*statementToExplain);
    auto op = make_shared<LogicalExplain>(planToExplain->getLastOperator(),
        statement.getStatementResult()->getSingleColumnExpr(), explain.getExplainType(),
        explain.getStatementToExplain()->getStatementResult()->getColumns());
    plan.setLastOperator(std::move(op));
}

void Planner::appendCreateMacro(const BoundStatement& statement, LogicalPlan& plan) {
    auto& createMacro = ku_dynamic_cast<const BoundStatement&, const BoundCreateMacro&>(statement);
    auto op = make_shared<LogicalCreateMacro>(statement.getStatementResult()->getSingleColumnExpr(),
        createMacro.getMacroName(), createMacro.getMacro());
    plan.setLastOperator(std::move(op));
}

void Planner::appendTransaction(const BoundStatement& statement, LogicalPlan& plan) {
    auto& transactionStatement =
        common::ku_dynamic_cast<const BoundStatement&, const BoundTransactionStatement&>(statement);
    auto op = std::make_shared<LogicalTransaction>(transactionStatement.getTransactionAction());
    plan.setLastOperator(std::move(op));
}

void Planner::appendExtension(const BoundStatement& statement, LogicalPlan& plan) {
    auto& extensionStatement =
        common::ku_dynamic_cast<const BoundStatement&, const BoundExtensionStatement&>(statement);
    auto op = std::make_shared<LogicalExtension>(extensionStatement.getAction(),
        extensionStatement.getPath(), statement.getStatementResult()->getSingleColumnExpr());
    plan.setLastOperator(std::move(op));
}

void Planner::appendAttachDatabase(const BoundStatement& statement, LogicalPlan& plan) {
    auto& boundAttachDatabase =
        common::ku_dynamic_cast<const binder::BoundStatement&, const binder::BoundAttachDatabase&>(
            statement);
    auto attachDatabase = std::make_shared<LogicalAttachDatabase>(
        boundAttachDatabase.getAttachInfo(), statement.getStatementResult()->getSingleColumnExpr());
    plan.setLastOperator(std::move(attachDatabase));
}

void Planner::appendDetachDatabase(const BoundStatement& statement, LogicalPlan& plan) {
    auto& boundDetachDatabase =
        common::ku_dynamic_cast<const binder::BoundStatement&, const binder::BoundDetachDatabase&>(
            statement);
    auto detachDatabase = std::make_shared<LogicalDetachDatabase>(boundDetachDatabase.getDBName(),
        statement.getStatementResult()->getSingleColumnExpr());
    plan.setLastOperator(std::move(detachDatabase));
}

void Planner::appendUseDatabase(const BoundStatement& statement, LogicalPlan& plan) {
    auto& boundUseDatabase =
        common::ku_dynamic_cast<const binder::BoundStatement&, const binder::BoundUseDatabase&>(
            statement);
    auto useDatabase = std::make_shared<LogicalUseDatabase>(boundUseDatabase.getDBName(),
        statement.getStatementResult()->getSingleColumnExpr());
    plan.setLastOperator(std::move(useDatabase));
}

} // namespace planner
} // namespace kuzu
