#include "parser/expression/parsed_expression.h"

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

} // namespace parser
} // namespace kuzu
