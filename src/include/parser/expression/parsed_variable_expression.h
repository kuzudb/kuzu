#pragma once

#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedVariableExpression : public ParsedExpression {
public:
    ParsedVariableExpression(std::string variableName, std::string raw)
        : ParsedExpression{common::VARIABLE, std::move(raw)}, variableName{
                                                                  std::move(variableName)} {}

    inline std::string getVariableName() const { return variableName; }

private:
    std::string variableName;
};

} // namespace parser
} // namespace kuzu
