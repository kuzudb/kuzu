#pragma once

#include "parser/expression/parsed_expression.h"
#include "reading_clause.h"

namespace kuzu {
namespace parser {

class UnwindClause : public ReadingClause {
public:
    UnwindClause(std::unique_ptr<ParsedExpression> expression, std::string listAlias)
        : ReadingClause{common::ClauseType::UNWIND}, expression{std::move(expression)},
          alias{std::move(listAlias)} {}

    const ParsedExpression* getExpression() const { return expression.get(); }

    std::string getAlias() const { return alias; }

private:
    std::unique_ptr<ParsedExpression> expression;
    std::string alias;
};

} // namespace parser
} // namespace kuzu
