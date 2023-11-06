#include "parser/expression/parsed_expression.h"

#include "common/exception/not_implemented.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "parser/expression/parsed_case_expression.h"
#include "parser/expression/parsed_function_expression.h"
#include "parser/expression/parsed_literal_expression.h"
#include "parser/expression/parsed_parameter_expression.h"
#include "parser/expression/parsed_property_expression.h"
#include "parser/expression/parsed_subquery_expression.h"
#include "parser/expression/parsed_variable_expression.h"

using namespace kuzu::common;

namespace kuzu {
namespace parser {

ParsedExpression::ParsedExpression(
    ExpressionType type, std::unique_ptr<ParsedExpression> child, std::string rawName)
    : type{type}, rawName{std::move(rawName)} {
    children.push_back(std::move(child));
}

ParsedExpression::ParsedExpression(ExpressionType type, std::unique_ptr<ParsedExpression> left,
    std::unique_ptr<ParsedExpression> right, std::string rawName)
    : type{type}, rawName{std::move(rawName)} {
    children.push_back(std::move(left));
    children.push_back(std::move(right));
}

parsed_expression_vector ParsedExpression::copyChildren() const {
    parsed_expression_vector childrenCopy;
    childrenCopy.reserve(children.size());
    for (auto& child : children) {
        childrenCopy.push_back(child->copy());
    }
    return childrenCopy;
}

void ParsedExpression::serialize(Serializer& serializer) const {
    serializer.serializeValue(type);
    serializer.serializeValue(alias);
    serializer.serializeValue(rawName);
    serializer.serializeVectorOfPtrs(children);
    serializeInternal(serializer);
}

std::unique_ptr<ParsedExpression> ParsedExpression::deserialize(Deserializer& deserializer) {
    ExpressionType type;
    std::string alias;
    std::string rawName;
    parsed_expression_vector children;
    deserializer.deserializeValue(type);
    deserializer.deserializeValue(alias);
    deserializer.deserializeValue(rawName);
    deserializer.deserializeVectorOfPtrs(children);
    std::unique_ptr<ParsedExpression> parsedExpression;
    switch (type) {
    case ExpressionType::CASE_ELSE: {
        parsedExpression = ParsedCaseExpression::deserialize(deserializer);
    } break;
    case ExpressionType::FUNCTION: {
        parsedExpression = ParsedFunctionExpression::deserialize(deserializer);
    } break;
    case ExpressionType::LITERAL: {
        parsedExpression = ParsedLiteralExpression::deserialize(deserializer);
    } break;
    case ExpressionType::PARAMETER: {
        parsedExpression = ParsedParameterExpression::deserialize(deserializer);
    } break;
    case ExpressionType::PROPERTY: {
        parsedExpression = ParsedPropertyExpression::deserialize(deserializer);
    } break;
    case ExpressionType::EXISTENTIAL_SUBQUERY: {
        parsedExpression = ParsedSubqueryExpression::deserialize(deserializer);
    } break;
    case ExpressionType::VARIABLE: {
        parsedExpression = ParsedVariableExpression::deserialize(deserializer);
    } break;
    default:
        throw NotImplementedException{"ParsedExpression::deserialize"};
    }
    parsedExpression->alias = std::move(alias);
    parsedExpression->rawName = std::move(rawName);
    parsedExpression->children = std::move(children);
    return parsedExpression;
}

} // namespace parser
} // namespace kuzu
