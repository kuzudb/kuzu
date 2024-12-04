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

BoundTableFunction Binder::bindTableFunc(std::string tableFuncName,
    const parser::ParsedExpression& expr, expression_vector& columns) {
    auto entry = BuiltInFunctionsUtils::getFunctionCatalogEntry(clientContext->getTx(),
        tableFuncName, clientContext->getCatalog()->getFunctions(clientContext->getTx()));
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
    std::vector<Value> inputValues;
    for (auto& param : positionalParams) {
        ExpressionUtil::validateExpressionType(*param, ExpressionType::LITERAL);
        auto literalExpr = param->constPtrCast<LiteralExpression>();
        inputValues.push_back(literalExpr->getValue());
    }
    auto tableFunc = func->constPtrCast<TableFunction>();
    for (auto i = 0u; i < positionalParams.size(); ++i) {
        auto parameterTypeID = tableFunc->parameterTypeIDs[i];
        ExpressionUtil::validateDataType(*positionalParams[i], parameterTypeID);
    }
    auto bindInput = function::ScanTableFuncBindInput();
    bindInput.inputs = std::move(inputValues);
    bindInput.optionalParams = std::move(optionalParams);
    auto bindData = tableFunc->bindFunc(clientContext, &bindInput);
    for (auto i = 0u; i < bindData->columnTypes.size(); i++) {
        columns.push_back(createVariable(bindData->columnNames[i], bindData->columnTypes[i]));
    }
    auto offset = expressionBinder.createVariableExpression(LogicalType::INT64(),
        std::string(InternalKeyword::ROW_OFFSET));
    return BoundTableFunction{tableFunc->copy(), std::move(bindData), std::move(offset)};
}

} // namespace binder
} // namespace kuzu
