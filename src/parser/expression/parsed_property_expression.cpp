#include "parser/expression/parsed_property_expression.h"

#include "common/ser_deser.h"

namespace kuzu {
namespace parser {

std::unique_ptr<ParsedPropertyExpression> ParsedPropertyExpression::deserialize(
    common::FileInfo* fileInfo, uint64_t& offset) {
    std::string propertyName;
    common::SerDeser::deserializeValue(propertyName, fileInfo, offset);
    return std::make_unique<ParsedPropertyExpression>(std::move(propertyName));
}

} // namespace parser
} // namespace kuzu
