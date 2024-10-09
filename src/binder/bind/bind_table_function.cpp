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
    expression_vector children;
    std::vector<LogicalType> childrenTypes;
    for (auto i = 0u; i < expr.getNumChildren(); i++) {
        auto child = expressionBinder.bindExpression(*expr.getChild(i));
        children.push_back(child);
        childrenTypes.push_back(child->getDataType().copy());
    }
    auto func = BuiltInFunctionsUtils::matchFunction(tableFuncName, childrenTypes, entry);
    std::vector<Value> inputValues;
    for (auto& param : children) {
        ExpressionUtil::validateExpressionType(*param, ExpressionType::LITERAL);
        auto literalExpr = param->constPtrCast<LiteralExpression>();
        inputValues.push_back(literalExpr->getValue());
    }
    auto tableFunc = func->constPtrCast<TableFunction>();
    for (auto i = 0u; i < children.size(); ++i) {
        auto parameterTypeID = tableFunc->parameterTypeIDs[i];
        ExpressionUtil::validateDataType(*children[i], parameterTypeID);
    }
    auto bindInput = function::ScanTableFuncBindInput();
    bindInput.inputs = std::move(inputValues);
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
