#pragma once

#include "parser/expression/parsed_expression.h"
#include "parser/query/reading_clause/reading_clause.h"

namespace kuzu {
namespace parser {

class InQueryCallClause final : public ReadingClause {
public:
    explicit InQueryCallClause(std::unique_ptr<ParsedExpression> functionExpression)
        : ReadingClause{common::ClauseType::IN_QUERY_CALL},
          functionExpression{std::move(functionExpression)} {}

    const ParsedExpression* getFunctionExpression() const { return functionExpression.get(); }

    void setWherePredicate(std::unique_ptr<ParsedExpression> expression) {
        wherePredicate = std::move(expression);
    }
    bool hasWherePredicate() const { return wherePredicate != nullptr; }
    const ParsedExpression* getWherePredicate() const { return wherePredicate.get(); }

private:
    std::unique_ptr<ParsedExpression> functionExpression;
    std::unique_ptr<ParsedExpression> wherePredicate;
};

} // namespace parser
} // namespace kuzu
