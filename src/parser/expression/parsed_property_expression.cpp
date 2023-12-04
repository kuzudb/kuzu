#include "parser/expression/parsed_property_expression.h"

#include "common/serializer/deserializer.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

std::unique_ptr<ParsedPropertyExpression> ParsedPropertyExpression::deserialize(
    Deserializer& deserializer) {
    std::string propertyName;
    deserializer.deserializeValue(propertyName);
    return std::make_unique<ParsedPropertyExpression>(std::move(propertyName));
}

} // namespace parser
} // namespace kuzu
