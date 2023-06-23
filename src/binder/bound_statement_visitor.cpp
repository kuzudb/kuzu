#include "binder/bound_statement_visitor.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

void BoundStatementVisitor::visit(const kuzu::binder::BoundStatement& statement) {
    switch (statement.getStatementType()) {
    case StatementType::QUERY: {
        visitRegularQuery((BoundRegularQuery&)statement);
    } break;
    case StatementType::CREATE_NODE_TABLE: {
        visitCreateNodeTable(statement);
    } break;
    case StatementType::CREATE_REL_TABLE: {
        visitCreateRelTable(statement);
    } break;
    case StatementType::DROP_TABLE: {
        visitDropTable(statement);
    } break;
    case StatementType::RENAME_TABLE: {
        visitRenameTable(statement);
    } break;
    case StatementType::ADD_PROPERTY: {
        visitAddProperty(statement);
    } break;
    case StatementType::DROP_PROPERTY: {
        visitDropProperty(statement);
    } break;
    case StatementType::RENAME_PROPERTY: {
        visitRenameProperty(statement);
    } break;
    case StatementType::COPY: {
        visitCopy(statement);
    } break;
    case StatementType::CALL: {
        visitCall(statement);
    } break;
    default:
        throw NotImplementedException("BoundStatementVisitor::visit");
    }
}

void BoundStatementVisitor::visitRegularQuery(const BoundRegularQuery& regularQuery) {
    for (auto i = 0u; i < regularQuery.getNumSingleQueries(); ++i) {
        visitSingleQuery(*regularQuery.getSingleQuery(i));
    }
}

void BoundStatementVisitor::visitSingleQuery(const NormalizedSingleQuery& singleQuery) {
    for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
        visitQueryPart(*singleQuery.getQueryPart(i));
    }
}

void BoundStatementVisitor::visitQueryPart(const NormalizedQueryPart& queryPart) {
    for (auto i = 0u; i < queryPart.getNumReadingClause(); ++i) {
        visitReadingClause(*queryPart.getReadingClause(i));
    }
    for (auto i = 0u; i < queryPart.getNumUpdatingClause(); ++i) {
        visitUpdatingClause(*queryPart.getUpdatingClause(i));
    }
    if (queryPart.hasProjectionBody()) {
        visitProjectionBody(*queryPart.getProjectionBody());
        if (queryPart.hasProjectionBodyPredicate()) {
            visitProjectionBodyPredicate(*queryPart.getProjectionBodyPredicate());
        }
    }
}

void BoundStatementVisitor::visitReadingClause(const BoundReadingClause& readingClause) {
    switch (readingClause.getClauseType()) {
    case common::ClauseType::MATCH: {
        visitMatch(readingClause);
    } break;
    case common::ClauseType::UNWIND: {
        visitUnwind(readingClause);
    } break;
    default:
        throw NotImplementedException("BoundStatementVisitor::visitReadingClause");
    }
}

void BoundStatementVisitor::visitUpdatingClause(const BoundUpdatingClause& updatingClause) {
    switch (updatingClause.getClauseType()) {
    case common::ClauseType::SET: {
        visitSet(updatingClause);
    } break;
    case common::ClauseType::DELETE_: {
        visitDelete(updatingClause);
    } break;
    case common::ClauseType::CREATE: {
        visitCreate(updatingClause);
    } break;
    default:
        throw NotImplementedException("BoundStatementVisitor::visitUpdatingClause");
    }
}

} // namespace binder
} // namespace kuzu
