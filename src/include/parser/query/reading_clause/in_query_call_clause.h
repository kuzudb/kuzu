#pragma once

#include "parser/expression/parsed_expression.h"
#include "parser/query/reading_clause/reading_clause.h"

namespace kuzu {
namespace parser {

class InQueryCallClause final : public ReadingClause {
public:
    explicit InQueryCallClause(std::unique_ptr<ParsedExpression> functionExpr)
        : ReadingClause{common::ClauseType::IN_QUERY_CALL},
          functionExpr{std::move(functionExpr)} {}

    const ParsedExpression* getFunctionExpression() const {
        return functionExpr.get();
    }

    void setWherePredicate(std::unique_ptr<ParsedExpression> expression) {
        wherePredicate = std::move(expression);
    }
    bool hasWherePredicate() const { return wherePredicate != nullptr; }
    const ParsedExpression* getWherePredicate() const { return wherePredicate.get(); }

private:
    std::unique_ptr<ParsedExpression> functionExpr;
    std::unique_ptr<ParsedExpression> wherePredicate;
};

} // namespace parser
} // namespace kuzu
