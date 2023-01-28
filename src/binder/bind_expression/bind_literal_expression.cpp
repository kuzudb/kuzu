#include "binder/binder.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression_binder.h"
#include "parser/expression/parsed_literal_expression.h"

namespace kuzu {
namespace binder {

shared_ptr<Expression> ExpressionBinder::bindLiteralExpression(
    const ParsedExpression& parsedExpression) {
    auto& literalExpression = (ParsedLiteralExpression&)parsedExpression;
    auto value = literalExpression.getValue();
    if (value->isNull()) {
        return bindNullLiteralExpression();
    }
    return make_shared<LiteralExpression>(value->copy());
}

shared_ptr<Expression> ExpressionBinder::bindNullLiteralExpression() {
    return make_shared<LiteralExpression>(
        make_unique<Value>(Value::createNullValue()), binder->getUniqueExpressionName("NULL"));
}

} // namespace binder
} // namespace kuzu
