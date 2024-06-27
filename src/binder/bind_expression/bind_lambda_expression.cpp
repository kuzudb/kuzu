#include "binder/binder.h"
#include "binder/expression/lambda_parameter_expression.h"
#include "binder/expression_binder.h"
#include "parser/expression/parsed_lambda_expression.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindLambdaExpression(
    const parser::ParsedExpression& parsedExpression, LogicalType varType) {
    auto& parsedLambdaExpression = parsedExpression.constCast<ParsedLambdaExpression>();
    auto prevScope = binder->saveScope();
    auto var = parsedLambdaExpression.getVariables();
    // TODO(Ziyi): Support multiple parameters.
    binder->addToScope(var->getRawName(), std::make_shared<LambdaParameterExpression>(
                                              varType.copy(), getUniqueName(var->getRawName())));
    auto boundLambdaExpr = bindExpression(*parsedLambdaExpression.getExpr());
    binder->restoreScope(std::move(prevScope));
    return boundLambdaExpr;
}

} // namespace binder
} // namespace kuzu
