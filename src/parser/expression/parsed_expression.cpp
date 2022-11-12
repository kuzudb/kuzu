#include "include/parsed_expression.h"

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

bool ParsedExpression::equals(const ParsedExpression& other) const {
    if (type != other.type || alias != other.alias || children.size() != other.children.size()) {
        return false;
    }
    for (auto i = 0u; i < children.size(); ++i) {
        if (*children[i] != *other.children[i]) {
            return false;
        }
    }
    return true;
}

} // namespace parser
} // namespace kuzu
