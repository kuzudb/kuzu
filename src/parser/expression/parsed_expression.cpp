#include "parser/expression/parsed_expression.h"

namespace kuzu {
namespace parser {

ParsedExpression::ParsedExpression(
    ExpressionType type, unique_ptr<ParsedExpression> child, string rawName)
    : type{type}, rawName{std::move(rawName)} {
    children.push_back(std::move(child));
}

ParsedExpression::ParsedExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
    unique_ptr<ParsedExpression> right, string rawName)
    : type{type}, rawName{std::move(rawName)} {
    children.push_back(std::move(left));
    children.push_back(std::move(right));
}

} // namespace parser
} // namespace kuzu
