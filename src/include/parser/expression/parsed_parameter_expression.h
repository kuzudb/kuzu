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

    inline std::unique_ptr<ParsedExpression> copy() const override {
        throw common::NotImplementedException{"ParsedParameterExpression::copy()"};
    }

private:
    std::string parameterName;
};

} // namespace parser
} // namespace kuzu
