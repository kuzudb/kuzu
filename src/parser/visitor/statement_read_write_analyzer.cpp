#include "parser/visitor/statement_read_write_analyzer.h"

#include "parser/expression/parsed_expression_visitor.h"
#include "parser/query/reading_clause/reading_clause.h"
#include "parser/query/return_with_clause/with_clause.h"

namespace kuzu {
namespace parser {

bool StatementReadWriteAnalyzer::isReadOnly(const Statement& statement) {
    visit(statement);
    return readOnly;
}

static bool hasSequenceUpdate(const ParsedExpression* expr) {
    auto collector = ParsedSequenceFunctionCollector();
    collector.visit(expr);
    return collector.hasSeqUpdate();
}

void StatementReadWriteAnalyzer::visitReadingClause(const ReadingClause* readingClause) {
    if (readingClause->hasWherePredicate()) {
        if (hasSequenceUpdate(readingClause->getWherePredicate())) {
            readOnly = false;
        }
    }
}

void StatementReadWriteAnalyzer::visitWithClause(const WithClause* withClause) {
    for (auto& expr : withClause->getProjectionBody()->getProjectionExpressions()) {
        if (hasSequenceUpdate(expr.get())) {
            readOnly = false;
            return;
        }
    }
}

void StatementReadWriteAnalyzer::visitReturnClause(const ReturnClause* returnClause) {
    for (auto& expr : returnClause->getProjectionBody()->getProjectionExpressions()) {
        if (hasSequenceUpdate(expr.get())) {
            readOnly = false;
            return;
        }
    }
}

} // namespace parser
} // namespace kuzu
