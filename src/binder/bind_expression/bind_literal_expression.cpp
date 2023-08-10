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
    auto& literalExpression = (ParsedLiteralExpression&)parsedExpression;
    auto value = literalExpression.getValue();
    if (value->isNull()) {
        return createNullLiteralExpression();
    }
    return createLiteralExpression(value->copy());
}

std::shared_ptr<Expression> ExpressionBinder::createLiteralExpression(
    std::unique_ptr<Value> value) {
    auto uniqueName = binder->getUniqueExpressionName(value->toString());
    return std::make_unique<LiteralExpression>(std::move(value), uniqueName);
}

std::shared_ptr<Expression> ExpressionBinder::createStringLiteralExpression(
    const std::string& strVal) {
    auto value = std::make_unique<Value>(LogicalType{LogicalTypeID::STRING}, strVal);
    return createLiteralExpression(std::move(value));
}

std::shared_ptr<Expression> ExpressionBinder::createNullLiteralExpression() {
    return make_shared<LiteralExpression>(
        std::make_unique<Value>(Value::createNullValue()), binder->getUniqueExpressionName("NULL"));
}

} // namespace binder
} // namespace kuzu
