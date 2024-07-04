#include "binder/binder.h"
#include "binder/expression/lambda_expression.h"
#include "binder/expression_binder.h"
#include "common/exception/binder.h"
#include "function/list/vector_list_functions.h"
#include "parser/expression/parsed_lambda_expression.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

void ExpressionBinder::bindLambdaExpression(const std::string& functionName,
    const Expression& lambdaInput, Expression& lambdaExpr) {
    if (functionName != function::ListTransformFunction::name &&
        functionName != function::ListFilterFunction::name &&
        functionName != function::ListReduceFunction::name) {
        throw BinderException(stringFormat("{} does not support lambda input.", functionName));
    }
    KU_ASSERT(lambdaInput.getDataType().getLogicalTypeID() == LogicalTypeID::LIST);
    auto& listChildType = ListType::getChildType(lambdaInput.getDataType());
    auto& boundLambdaExpr = lambdaExpr.cast<BoundLambdaExpression>();
    auto& parsedLambdaExpr =
        boundLambdaExpr.getParsedLambdaExpr()->constCast<ParsedLambdaExpression>();
    auto prevScope = binder->saveScope();
    // TODO(Xiyang): we should not clear scope here.
    binder->scope.clear();
    for (auto& varName : parsedLambdaExpr.getVarNames()) {
        binder->createVariable(varName, listChildType);
    }
    auto funcExpr =
        binder->getExpressionBinder()->bindExpression(*parsedLambdaExpr.getFunctionExpr());
    binder->restoreScope(std::move(prevScope));
    boundLambdaExpr.cast(funcExpr->getDataType().copy());
    boundLambdaExpr.setFunctionExpr(std::move(funcExpr));
}

std::shared_ptr<Expression> ExpressionBinder::bindLambdaExpression(
    const parser::ParsedExpression& parsedExpr) const {
    auto uniqueName = getUniqueName(parsedExpr.getRawName());
    return std::make_shared<BoundLambdaExpression>(parsedExpr.copy(), uniqueName);
}

} // namespace binder
} // namespace kuzu
