#pragma once

#include "common/ser_deser.h"
#include "parsed_expression.h"

namespace kuzu {
namespace parser {

class ParsedVariableExpression : public ParsedExpression {
public:
    ParsedVariableExpression(std::string variableName, std::string raw)
        : ParsedExpression{common::VARIABLE, std::move(raw)}, variableName{
                                                                  std::move(variableName)} {}

    ParsedVariableExpression(std::string alias, std::string rawName,
        parsed_expression_vector children, std::string variableName)
        : ParsedExpression{common::VARIABLE, std::move(alias), std::move(rawName),
              std::move(children)},
          variableName{std::move(variableName)} {}

    ParsedVariableExpression(std::string variableName)
        : ParsedExpression{common::VARIABLE}, variableName{std::move(variableName)} {}

    inline std::string getVariableName() const { return variableName; }

    static std::unique_ptr<ParsedVariableExpression> deserialize(
        common::FileInfo* fileInfo, uint64_t& offset);

    inline std::unique_ptr<ParsedExpression> copy() const override {
        return std::make_unique<ParsedVariableExpression>(
            alias, rawName, copyChildren(), variableName);
    }

private:
    inline void serializeInternal(common::FileInfo* fileInfo, uint64_t& offset) const override {
        common::SerDeser::serializeValue(variableName, fileInfo, offset);
    }

private:
    std::string variableName;
};

} // namespace parser
} // namespace kuzu
