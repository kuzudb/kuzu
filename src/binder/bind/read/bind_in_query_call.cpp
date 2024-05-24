#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "binder/query/reading_clause/bound_gds_call.h"
#include "binder/query/reading_clause/bound_table_function_call.h"
#include "catalog/catalog.h"
#include "common/exception/binder.h"
#include "function/built_in_function_utils.h"
#include "function/gds_function.h"
#include "parser/expression/parsed_function_expression.h"
#include "parser/query/reading_clause/in_query_call_clause.h"

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::parser;
using namespace kuzu::function;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundReadingClause> Binder::bindInQueryCall(const ReadingClause& readingClause) {
    auto& call = readingClause.constCast<InQueryCallClause>();
    auto expr = call.getFunctionExpression();
    auto functionExpr = expr->constPtrCast<ParsedFunctionExpression>();
    auto functionName = functionExpr->getFunctionName();
    expression_vector children;
    std::vector<LogicalType> childrenTypes;
    for (auto i = 0u; i < functionExpr->getNumChildren(); i++) {
        auto child = expressionBinder.bindExpression(*functionExpr->getChild(i));
        children.push_back(child);
        childrenTypes.push_back(child->getDataType());
    }
    std::unique_ptr<BoundReadingClause> boundReadingClause;
    expression_vector outExprs;
    auto catalogSet = clientContext->getCatalog()->getFunctions(clientContext->getTx());
    auto entry = BuiltInFunctionsUtils::getFunctionCatalogEntry(clientContext->getTx(),
        functionName, catalogSet);
    auto func = BuiltInFunctionsUtils::matchFunction(functionName, childrenTypes, entry);
    switch (entry->getType()) {
    case CatalogEntryType::TABLE_FUNCTION_ENTRY: {
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
        auto bindInput = function::TableFuncBindInput();
        bindInput.inputs = std::move(inputValues);
        auto bindData = tableFunc->bindFunc(clientContext, &bindInput);
        for (auto i = 0u; i < bindData->columnTypes.size(); i++) {
            outExprs.push_back(createVariable(bindData->columnNames[i], bindData->columnTypes[i]));
        }
        auto offset = expressionBinder.createVariableExpression(*LogicalType::INT64(),
            std::string(InternalKeyword::ROW_OFFSET));
        boundReadingClause = std::make_unique<BoundTableFunctionCall>(*tableFunc,
            std::move(bindData), std::move(offset), std::move(outExprs));
    } break;
    case CatalogEntryType::GDS_FUNCTION_ENTRY: {
        auto gdsFunc = *func->constPtrCast<GDSFunction>();
        gdsFunc.gds->bind(children);
        auto columnNames = gdsFunc.gds->getResultColumnNames();
        auto columnTypes = gdsFunc.gds->getResultColumnTypes();
        for (auto i = 0u; i < columnNames.size(); ++i) {
            outExprs.push_back(createVariable(columnNames[i], columnTypes[i]));
        }
        if (children.empty()) {
            throw BinderException(
                stringFormat("{} function requires at least one input", functionName));
        }
        // Validate first child is a graph expression.
        auto graphExpr = children[0];
        ExpressionUtil::validateExpressionType(*graphExpr, ExpressionType::GRAPH);
        boundReadingClause =
            std::make_unique<BoundGDSCall>(gdsFunc, std::move(graphExpr), std::move(outExprs));
    } break;
    default:
        throw BinderException(
            stringFormat("{} is not a table or algorithm function.", functionName));
    }
    if (call.hasWherePredicate()) {
        auto wherePredicate = bindWhereExpression(*call.getWherePredicate());
        boundReadingClause->setPredicate(std::move(wherePredicate));
    }
    return boundReadingClause;
}

} // namespace binder
} // namespace kuzu
