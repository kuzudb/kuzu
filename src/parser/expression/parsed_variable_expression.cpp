#include "parser/expression/parsed_variable_expression.h"

#include "common/ser_deser.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::unique_ptr<ParsedVariableExpression> ParsedVariableExpression::deserialize(
    FileInfo* fileInfo, uint64_t& offset) {
    std::string variableName;
    SerDeser::deserializeValue(variableName, fileInfo, offset);
    return std::make_unique<ParsedVariableExpression>(std::move(variableName));
}

} // namespace parser
} // namespace kuzu
