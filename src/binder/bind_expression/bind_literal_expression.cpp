#include "binder/binder.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression_binder.h"
#include "parser/expression/parsed_literal_expression.h"

using namespace kuzu::parser;
using namespace kuzu::common;

namespace kuzu {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindLiteralExpression(
    const ParsedExpression& parsedExpression) {
    auto& literalExpression = parsedExpression.constCast<ParsedLiteralExpression>();
    auto value = literalExpression.getValue();
    if (value.isNull()) {
        return createNullLiteralExpression();
    }
    return createLiteralExpression(value);
}

std::shared_ptr<Expression> ExpressionBinder::createLiteralExpression(const Value& value) {
    auto uniqueName = binder->getUniqueExpressionName(value.toString());
    return std::make_unique<LiteralExpression>(std::move(value), uniqueName);
}

std::shared_ptr<Expression> ExpressionBinder::createLiteralExpression(const std::string& strVal) {
    return createLiteralExpression(Value(strVal));
}

std::shared_ptr<Expression> ExpressionBinder::createNullLiteralExpression() {
    return make_shared<LiteralExpression>(Value::createNullValue(),
        binder->getUniqueExpressionName("NULL"));
}

} // namespace binder
} // namespace kuzu
