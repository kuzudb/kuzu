#pragma once

#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedParameterExpression : public ParsedExpression {
public:
    explicit ParsedParameterExpression(std::string parameterName, std::string raw)
        : ParsedExpression{common::PARAMETER, std::move(raw)}, parameterName{
                                                                   std::move(parameterName)} {}

    inline std::string getParameterName() const { return parameterName; }

private:
    std::string parameterName;
};

} // namespace parser
} // namespace kuzu
