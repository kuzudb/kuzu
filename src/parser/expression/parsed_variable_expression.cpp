#include "parser/expression/parsed_variable_expression.h"

#include "common/ser_deser.h"

namespace kuzu {
namespace parser {

std::unique_ptr<ParsedVariableExpression> ParsedVariableExpression::deserialize(
    common::FileInfo* fileInfo, uint64_t& offset) {
    std::string variableName;
    common::SerDeser::deserializeValue(variableName, fileInfo, offset);
    return std::make_unique<ParsedVariableExpression>(std::move(variableName));
}

} // namespace parser
} // namespace kuzu
