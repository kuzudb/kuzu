#include "parser/expression/parsed_function_expression.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::unique_ptr<ParsedFunctionExpression> ParsedFunctionExpression::deserialize(
    Deserializer& deserializer) {
    bool isDistinct;
    deserializer.deserializeValue(isDistinct);
    std::string functionName;
    deserializer.deserializeValue(functionName);
    return std::make_unique<ParsedFunctionExpression>(isDistinct, std::move(functionName));
}

void ParsedFunctionExpression::serializeInternal(Serializer& serializer) const {
    serializer.serializeValue(isDistinct);
    serializer.serializeValue(functionName);
}

} // namespace parser
} // namespace kuzu
