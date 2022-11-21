#include "parser/expression/parsed_expression.h"

namespace kuzu {
namespace parser {

ParsedExpression::ParsedExpression(
    ExpressionType type, unique_ptr<ParsedExpression> child, string rawName)
    : type{type}, rawName{move(rawName)} {
    children.push_back(move(child));
}

ParsedExpression::ParsedExpression(ExpressionType type, unique_ptr<ParsedExpression> left,
    unique_ptr<ParsedExpression> right, string rawName)
    : type{type}, rawName{move(rawName)} {
    children.push_back(move(left));
    children.push_back(move(right));
}

} // namespace parser
} // namespace kuzu
