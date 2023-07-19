#pragma once

#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedVariableExpression : public ParsedExpression {
public:
    ParsedVariableExpression(std::string variableName, std::string raw)
        : ParsedExpression{common::VARIABLE, std::move(raw)}, variableName{
                                                                  std::move(variableName)} {}

    ParsedVariableExpression(common::ExpressionType type, std::string alias, std::string rawName,
        parsed_expression_vector children, std::string variableName)
        : ParsedExpression{type, std::move(alias), std::move(rawName), std::move(children)},
          variableName{std::move(variableName)} {}

    inline std::string getVariableName() const { return variableName; }

    inline std::unique_ptr<ParsedExpression> copy() const override {
        return std::make_unique<ParsedVariableExpression>(
            type, alias, rawName, copyChildren(), variableName);
    }

private:
    std::string variableName;
};

} // namespace parser
} // namespace kuzu
