#pragma once

#include "parser/expression/parsed_expression.h"
#include "parser/query/reading_clause/reading_clause.h"

namespace kuzu {
namespace parser {

class InQueryCallClause final : public ReadingClause {
    static constexpr common::ClauseType clauseType_ = common::ClauseType::IN_QUERY_CALL;

public:
    explicit InQueryCallClause(std::unique_ptr<ParsedExpression> functionExpression)
        : ReadingClause{clauseType_}, functionExpression{std::move(functionExpression)} {}

    const ParsedExpression* getFunctionExpression() const { return functionExpression.get(); }

private:
    std::unique_ptr<ParsedExpression> functionExpression;
};

} // namespace parser
} // namespace kuzu
