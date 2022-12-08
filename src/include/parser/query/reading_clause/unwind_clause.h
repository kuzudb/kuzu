#pragma once

#include "parser/expression/parsed_expression.h"
#include "reading_clause.h"

namespace kuzu {
namespace parser {

class UnwindClause : public ReadingClause {

public:
    explicit UnwindClause(unique_ptr<ParsedExpression> expression, string listAlias)
        : ReadingClause{ClauseType::UNWIND}, expression{move(expression)}, alias{move(listAlias)} {}

    ~UnwindClause() = default;

    inline ParsedExpression* getExpression() const { return expression.get(); }

    inline const string getAlias() const { return alias; }

private:
    unique_ptr<ParsedExpression> expression;
    string alias;
};

} // namespace parser
} // namespace kuzu
