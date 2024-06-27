#pragma once

#include "common/enums/expression_type.h"
#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedLambdaExpression : public ParsedExpression {
public:
    static constexpr const common::ExpressionType TYPE = common::ExpressionType::LAMBDA;

public:
    ParsedLambdaExpression(std::unique_ptr<ParsedExpression> variables,
        std::unique_ptr<ParsedExpression> expr, std::string rawName)
        : ParsedExpression{TYPE, rawName}, variables{std::move(variables)}, expr{std::move(expr)} {}

    ParsedExpression* getVariables() const { return variables.get(); }

    ParsedExpression* getExpr() const { return expr.get(); }

private:
    std::unique_ptr<ParsedExpression> variables;
    std::unique_ptr<ParsedExpression> expr;
};

} // namespace parser
} // namespace kuzu
