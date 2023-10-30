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

private:
    std::unique_ptr<ParsedExpression> functionExpression;
};

} // namespace parser
} // namespace kuzu
