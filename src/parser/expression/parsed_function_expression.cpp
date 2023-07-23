#include "parser/expression/parsed_function_expression.h"

#include "common/ser_deser.h"

namespace kuzu {
namespace parser {

std::unique_ptr<ParsedFunctionExpression> ParsedFunctionExpression::deserialize(
    common::FileInfo* fileInfo, uint64_t& offset) {
    bool isDistinct;
    common::SerDeser::deserializeValue(isDistinct, fileInfo, offset);
    std::string functionName;
    common::SerDeser::deserializeValue(functionName, fileInfo, offset);
    return std::make_unique<ParsedFunctionExpression>(isDistinct, std::move(functionName));
}

void ParsedFunctionExpression::serializeInternal(
    common::FileInfo* fileInfo, uint64_t& offset) const {
    common::SerDeser::serializeValue(isDistinct, fileInfo, offset);
    common::SerDeser::serializeValue(functionName, fileInfo, offset);
}

} // namespace parser
} // namespace kuzu
