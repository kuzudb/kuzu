#include "binder/bound_statement_visitor.h"

#include "binder/bound_explain.h"
#include "binder/query/bound_regular_query.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

void BoundStatementVisitor::visit(const kuzu::binder::BoundStatement& statement) {
    switch (statement.getStatementType()) {
    case StatementType::QUERY: {
        visitRegularQuery(statement);
    } break;
    case StatementType::CREATE_TABLE: {
        visitCreateTable(statement);
    } break;
    case StatementType::DROP_TABLE: {
        visitDropTable(statement);
    } break;
    case StatementType::ALTER: {
        visitAlter(statement);
    } break;
    case StatementType::COPY_FROM: {
        visitCopyFrom(statement);
    } break;
    case StatementType::COPY_TO: {
        visitCopyTo(statement);
    } break;
    case StatementType::STANDALONE_CALL: {
        visitStandaloneCall(statement);
    } break;
    case StatementType::COMMENT_ON: {
        visitCommentOn(statement);
    } break;
    case StatementType::EXPLAIN: {
        visitExplain(statement);
    } break;
    case StatementType::CREATE_MACRO: {
        visitCreateMacro(statement);
    } break;
    case StatementType::TRANSACTION: {
        visitTransaction(statement);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void BoundStatementVisitor::visitRegularQuery(const BoundStatement& statement) {
    auto& regularQuery = reinterpret_cast<const BoundRegularQuery&>(statement);
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
            visitProjectionBodyPredicate(queryPart.getProjectionBodyPredicate());
        }
    }
}

void BoundStatementVisitor::visitExplain(const BoundStatement& statement) {
    visit(*((const BoundExplain&)statement).getStatementToExplain());
}

void BoundStatementVisitor::visitReadingClause(const BoundReadingClause& readingClause) {
    switch (readingClause.getClauseType()) {
    case ClauseType::MATCH: {
        visitMatch(readingClause);
    } break;
    case ClauseType::UNWIND: {
        visitUnwind(readingClause);
    } break;
    case ClauseType::IN_QUERY_CALL: {
        visitInQueryCall(readingClause);
    } break;
    case ClauseType::LOAD_FROM: {
        visitLoadFrom(readingClause);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void BoundStatementVisitor::visitUpdatingClause(const BoundUpdatingClause& updatingClause) {
    switch (updatingClause.getClauseType()) {
    case ClauseType::SET: {
        visitSet(updatingClause);
    } break;
    case ClauseType::DELETE_: {
        visitDelete(updatingClause);
    } break;
    case ClauseType::INSERT: {
        visitInsert(updatingClause);
    } break;
    case ClauseType::MERGE: {
        visitMerge(updatingClause);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

} // namespace binder
} // namespace kuzu
