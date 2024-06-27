#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/lambda_parameter_expression.h"
#include "binder/expression/property_expression.h"
#include "binder/expression_binder.h"
#include "catalog/catalog.h"
#include "common/exception/binder.h"
#include "function/aggregate/collect.h"
#include "function/built_in_function_utils.h"
#include "function/rewrite_function.h"
#include "function/scalar_macro_function.h"
#include "main/client_context.h"
#include "parser/expression/parsed_function_expression.h"
#include "parser/expression/parsed_lambda_expression.h"
#include "parser/parsed_expression_visitor.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::function;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindLambdaExpression(
    const parser::ParsedExpression& parsedExpression, LogicalType varType) {
    auto& parsedLambdaExpression = parsedExpression.constCast<ParsedLambdaExpression>();
    auto prevScope = binder->saveScope();
    auto var = parsedLambdaExpression.getVariables();
    binder->addToScope(var->getRawName(), std::make_shared<LambdaParameterExpression>(
                                              varType.copy(), getUniqueName(var->getRawName())));
    auto boundLambdaExpr = bindExpression(*parsedLambdaExpression.getExpr());
    binder->restoreScope(std::move(prevScope));
    return boundLambdaExpr;
}

} // namespace binder
} // namespace kuzu
