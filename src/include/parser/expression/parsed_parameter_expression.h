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

    static std::unique_ptr<ParsedParameterExpression> deserialize(
        common::FileInfo* fileInfo, uint64_t& offset) {
        throw common::NotImplementedException{"ParsedParameterExpression::deserialize()"};
    }

    inline std::unique_ptr<ParsedExpression> copy() const override {
        throw common::NotImplementedException{"ParsedParameterExpression::copy()"};
    }

private:
    void serializeInternal(common::FileInfo* fileInfo, uint64_t& offset) const override {
        throw common::NotImplementedException{"ParsedParameterExpression::serializeInternal()"};
    }

private:
    std::string parameterName;
};

} // namespace parser
} // namespace kuzu
