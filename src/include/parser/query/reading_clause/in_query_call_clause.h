#pragma once

#include "parser/expression/parsed_expression.h"
#include "parser/query/reading_clause/reading_clause.h"

namespace kuzu {
namespace parser {

class InQueryCallClause : public ReadingClause {
public:
    InQueryCallClause(std::unique_ptr<ParsedExpression> functionExpression)
        : ReadingClause{common::ClauseType::IN_QUERY_CALL}, functionExpression{
                                                                std::move(functionExpression)} {}
    ParsedExpression* getFunctionExpression() const { return functionExpression.get(); }

    inline void setWherePredicate(std::unique_ptr<ParsedExpression> expression) {
        wherePredicate = std::move(expression);
    }
    inline bool hasWherePredicate() const { return wherePredicate != nullptr; }
    inline ParsedExpression* getWherePredicate() const { return wherePredicate.get(); }

private:
    std::unique_ptr<ParsedExpression> functionExpression;
    std::unique_ptr<ParsedExpression> wherePredicate;
};

} // namespace parser
} // namespace kuzu
