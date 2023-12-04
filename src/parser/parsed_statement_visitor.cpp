#include "parser/parsed_statement_visitor.h"

#include "common/assert.h"
#include "parser/explain_statement.h"
#include "parser/query/regular_query.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

void StatementVisitor::visit(const Statement& statement) {
    switch (statement.getStatementType()) {
    case StatementType::QUERY: {
        visitQuery(statement);
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
    case StatementType::EXPLAIN: {
        visitExplain(statement);
    } break;
    case StatementType::CREATE_MACRO: {
        visitCreateMacro(statement);
    } break;
    case StatementType::COMMENT_ON: {
        visitCommentOn(statement);
    } break;
    case StatementType::TRANSACTION: {
        visitTransaction(statement);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void StatementVisitor::visitExplain(const Statement& statement) {
    auto& explainStatement = reinterpret_cast<const ExplainStatement&>(statement);
    visit(*explainStatement.getStatementToExplain());
}

void StatementVisitor::visitQuery(const Statement& statement) {
    auto& regularQuery = reinterpret_cast<const RegularQuery&>(statement);
    for (auto i = 0u; i < regularQuery.getNumSingleQueries(); ++i) {
        visitSingleQuery(regularQuery.getSingleQuery(i));
    }
}

void StatementVisitor::visitSingleQuery(const SingleQuery* singleQuery) {
    for (auto i = 0u; i < singleQuery->getNumQueryParts(); ++i) {
        visitQueryPart(singleQuery->getQueryPart(i));
    }
    for (auto i = 0u; i < singleQuery->getNumReadingClauses(); ++i) {
        visitReadingClause(singleQuery->getReadingClause(i));
    }
    for (auto i = 0u; i < singleQuery->getNumUpdatingClauses(); ++i) {
        visitUpdatingClause(singleQuery->getUpdatingClause(i));
    }
    visitReturnClause(singleQuery->getReturnClause());
}

void StatementVisitor::visitQueryPart(const QueryPart* queryPart) {
    for (auto i = 0u; i < queryPart->getNumReadingClauses(); ++i) {
        visitReadingClause(queryPart->getReadingClause(i));
    }
    for (auto i = 0u; i < queryPart->getNumUpdatingClauses(); ++i) {
        visitUpdatingClause(queryPart->getUpdatingClause(i));
    }
    visitWithClause(queryPart->getWithClause());
}

void StatementVisitor::visitReadingClause(const ReadingClause* readingClause) {
    switch (readingClause->getClauseType()) {
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

void StatementVisitor::visitUpdatingClause(const UpdatingClause* updatingClause) {
    switch (updatingClause->getClauseType()) {
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

} // namespace parser
} // namespace kuzu
