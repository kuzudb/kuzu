#include "parser/expression/parsed_variable_expression.h"

#include "common/serializer/deserializer.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::unique_ptr<ParsedVariableExpression> ParsedVariableExpression::deserialize(
    Deserializer& deserializer) {
    std::string variableName;
    deserializer.deserializeValue(variableName);
    return std::make_unique<ParsedVariableExpression>(std::move(variableName));
}

} // namespace parser
} // namespace kuzu
