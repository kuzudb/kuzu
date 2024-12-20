#include "binder/binder.h"
#include "binder/bound_table_function.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "catalog/catalog.h"
#include "function/built_in_function_utils.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace binder {

static void validateParameterType(expression_vector positionalParams) {
    for (auto& param : positionalParams) {
        ExpressionUtil::validateExpressionType(*param,
            {ExpressionType::LITERAL, ExpressionType::PARAMETER});
    }
}

BoundTableFunction Binder::bindTableFunc(std::string tableFuncName,
    const parser::ParsedExpression& expr, expression_vector& columns) {
    auto functions = clientContext->getCatalog()->getFunctions(clientContext->getTx());
    auto entry = BuiltInFunctionsUtils::getFunctionCatalogEntry(clientContext->getTx(),
        tableFuncName, functions);
    expression_vector positionalParams;
    std::vector<LogicalType> positionalParamTypes;
    optional_params_t optionalParams;
    for (auto i = 0u; i < expr.getNumChildren(); i++) {
        auto& childExpr = *expr.getChild(i);
        auto param = expressionBinder.bindExpression(childExpr);
        if (!childExpr.hasAlias()) {
            positionalParams.push_back(param);
            positionalParamTypes.push_back(param->getDataType().copy());
        } else {
            ExpressionUtil::validateExpressionType(*param, ExpressionType::LITERAL);
            auto literalExpr = param->constPtrCast<LiteralExpression>();
            optionalParams.emplace(childExpr.getAlias(), literalExpr->getValue());
        }
    }
    auto func = BuiltInFunctionsUtils::matchFunction(tableFuncName, positionalParamTypes, entry);
    validateParameterType(positionalParams);
    auto tableFunc = func->constPtrCast<TableFunction>();
    for (auto i = 0u; i < positionalParams.size(); ++i) {
        auto parameterTypeID = tableFunc->parameterTypeIDs[i];
        if (positionalParams[i]->expressionType == ExpressionType::LITERAL) {
            ExpressionUtil::validateDataType(*positionalParams[i], parameterTypeID);
        }
    }
    auto bindInput = TableFuncBindInput();
    bindInput.params = std::move(positionalParams);
    bindInput.optionalParams = std::move(optionalParams);
    bindInput.binder = this;
    auto bindData = tableFunc->bindFunc(clientContext, &bindInput);
    columns = bindData->columns;
    return BoundTableFunction{tableFunc->copy(), std::move(bindData)};
}

} // namespace binder
} // namespace kuzu
