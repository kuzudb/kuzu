#include "parser/expression/parsed_function_expression.h"

#include "common/ser_deser.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::unique_ptr<ParsedFunctionExpression> ParsedFunctionExpression::deserialize(
    FileInfo* fileInfo, uint64_t& offset) {
    bool isDistinct;
    SerDeser::deserializeValue(isDistinct, fileInfo, offset);
    std::string functionName;
    SerDeser::deserializeValue(functionName, fileInfo, offset);
    return std::make_unique<ParsedFunctionExpression>(isDistinct, std::move(functionName));
}

void ParsedFunctionExpression::serializeInternal(FileInfo* fileInfo, uint64_t& offset) const {
    SerDeser::serializeValue(isDistinct, fileInfo, offset);
    SerDeser::serializeValue(functionName, fileInfo, offset);
}

} // namespace parser
} // namespace kuzu
