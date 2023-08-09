#include "parser/expression/parsed_property_expression.h"

#include "common/ser_deser.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::unique_ptr<ParsedPropertyExpression> ParsedPropertyExpression::deserialize(
    FileInfo* fileInfo, uint64_t& offset) {
    std::string propertyName;
    SerDeser::deserializeValue(propertyName, fileInfo, offset);
    return std::make_unique<ParsedPropertyExpression>(std::move(propertyName));
}

} // namespace parser
} // namespace kuzu
