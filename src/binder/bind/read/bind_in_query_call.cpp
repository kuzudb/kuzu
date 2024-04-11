#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "binder/query/reading_clause/bound_in_query_call.h"
#include "catalog/catalog.h"
#include "function/built_in_function_utils.h"
#include "parser/expression/parsed_function_expression.h"
#include "parser/query/reading_clause/in_query_call_clause.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::function;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundReadingClause> Binder::bindInQueryCall(const ReadingClause& readingClause) {
    auto& call = readingClause.constCast<InQueryCallClause>();
    auto expr = call.getFunctionExpression();
    auto functionExpr = expr->constPtrCast<ParsedFunctionExpression>();
    expression_vector params;
    for (auto i = 0u; i < functionExpr->getNumChildren(); i++) {
        auto child = functionExpr->getChild(i);
        params.push_back(expressionBinder.bindExpression(*child));
    }
    TableFunction tableFunction;
    std::vector<Value> inputValues;
    std::vector<LogicalType> inputTypes;
    for (auto& param : params) {
        ExpressionUtil::validateExpressionType(*param, ExpressionType::LITERAL);
        auto literalExpr = param->constPtrCast<LiteralExpression>();
        inputTypes.push_back(literalExpr->getDataType());
        inputValues.push_back(*literalExpr->getValue());
    }
    auto functions = clientContext->getCatalog()->getFunctions(clientContext->getTx());
    auto func = BuiltInFunctionsUtils::matchFunction(functionExpr->getFunctionName(), inputTypes,
        functions);
    tableFunction = *ku_dynamic_cast<function::Function*, function::TableFunction*>(func);
    auto bindInput = function::TableFuncBindInput();
    bindInput.inputs = std::move(inputValues);
    auto bindData = tableFunction.bindFunc(clientContext, &bindInput);
    expression_vector columns;
    for (auto i = 0u; i < bindData->columnTypes.size(); i++) {
        columns.push_back(createVariable(bindData->columnNames[i], bindData->columnTypes[i]));
    }
    auto offset = expressionBinder.createVariableExpression(*LogicalType::INT64(),
        std::string(InternalKeyword::ROW_OFFSET));
    auto boundInQueryCall = std::make_unique<BoundInQueryCall>(tableFunction, std::move(bindData),
        std::move(columns), offset);
    if (call.hasWherePredicate()) {
        auto wherePredicate = expressionBinder.bindExpression(*call.getWherePredicate());
        boundInQueryCall->setPredicate(std::move(wherePredicate));
    }
    return boundInQueryCall;
}

} // namespace binder
} // namespace kuzu
