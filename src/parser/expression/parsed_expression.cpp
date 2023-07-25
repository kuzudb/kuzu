#include "parser/expression/parsed_expression.h"

#include "common/exception.h"
#include "common/ser_deser.h"
#include "parser/expression/parsed_case_expression.h"
#include "parser/expression/parsed_function_expression.h"
#include "parser/expression/parsed_literal_expression.h"
#include "parser/expression/parsed_parameter_expression.h"
#include "parser/expression/parsed_property_expression.h"
#include "parser/expression/parsed_subquery_expression.h"
#include "parser/expression/parsed_variable_expression.h"

namespace kuzu {
namespace parser {

ParsedExpression::ParsedExpression(
    common::ExpressionType type, std::unique_ptr<ParsedExpression> child, std::string rawName)
    : type{type}, rawName{std::move(rawName)} {
    children.push_back(std::move(child));
}

ParsedExpression::ParsedExpression(common::ExpressionType type,
    std::unique_ptr<ParsedExpression> left, std::unique_ptr<ParsedExpression> right,
    std::string rawName)
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

void ParsedExpression::serialize(common::FileInfo* fileInfo, uint64_t& offset) const {
    common::SerDeser::serializeValue(type, fileInfo, offset);
    common::SerDeser::serializeValue(alias, fileInfo, offset);
    common::SerDeser::serializeValue(rawName, fileInfo, offset);
    common::SerDeser::serializeVectorOfPtrs(children, fileInfo, offset);
    serializeInternal(fileInfo, offset);
}

std::unique_ptr<ParsedExpression> ParsedExpression::deserialize(
    common::FileInfo* fileInfo, uint64_t& offset) {
    common::ExpressionType type;
    std::string alias;
    std::string rawName;
    parsed_expression_vector children;
    common::SerDeser::deserializeValue(type, fileInfo, offset);
    common::SerDeser::deserializeValue(alias, fileInfo, offset);
    common::SerDeser::deserializeValue(rawName, fileInfo, offset);
    common::SerDeser::deserializeVectorOfPtrs(children, fileInfo, offset);
    std::unique_ptr<ParsedExpression> parsedExpression;
    switch (type) {
    case common::ExpressionType::CASE_ELSE: {
        parsedExpression = ParsedCaseExpression::deserialize(fileInfo, offset);
    } break;
    case common::ExpressionType::FUNCTION: {
        parsedExpression = ParsedFunctionExpression::deserialize(fileInfo, offset);
    } break;
    case common::ExpressionType::LITERAL: {
        parsedExpression = ParsedLiteralExpression::deserialize(fileInfo, offset);
    } break;
    case common::ExpressionType::PARAMETER: {
        parsedExpression = ParsedParameterExpression::deserialize(fileInfo, offset);
    } break;
    case common::ExpressionType::PROPERTY: {
        parsedExpression = ParsedPropertyExpression::deserialize(fileInfo, offset);
    } break;
    case common::ExpressionType::EXISTENTIAL_SUBQUERY: {
        parsedExpression = ParsedSubqueryExpression::deserialize(fileInfo, offset);
    } break;
    case common::ExpressionType::VARIABLE: {
        parsedExpression = ParsedVariableExpression::deserialize(fileInfo, offset);
    } break;
    default:
        throw common::NotImplementedException{"ParsedExpression::deserialize"};
    }
    parsedExpression->alias = std::move(alias);
    parsedExpression->rawName = std::move(rawName);
    parsedExpression->children = std::move(children);
    return parsedExpression;
}

} // namespace parser
} // namespace kuzu
