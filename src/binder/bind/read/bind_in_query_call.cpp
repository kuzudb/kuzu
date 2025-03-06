#include "binder/binder.h"
#include "binder/expression/expression_util.h"
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
    std::unique_ptr<BoundReadingClause> boundReadingClause;
    expression_vector columns;
    auto entry = clientContext->getCatalog()->getFunctionEntry(clientContext->getTransaction(),
        functionName);
    switch (entry->getType()) {
    case CatalogEntryType::TABLE_FUNCTION_ENTRY: {
        auto boundTableFunction =
            bindTableFunc(functionName, *functionExpr, columns, call.getYieldVariables());
        boundReadingClause = std::make_unique<BoundTableFunctionCall>(std::move(boundTableFunction),
            std::move(columns));
    } break;
    case CatalogEntryType::GDS_FUNCTION_ENTRY: {
        expression_vector children;
        std::vector<LogicalType> childrenTypes;
        expression_vector optionalParams;
        for (auto i = 0u; i < functionExpr->getNumChildren(); i++) {
            auto child = expressionBinder.bindExpression(*functionExpr->getChild(i));
            if (!functionExpr->getChild(i)->hasAlias()) {
                children.push_back(child);
                childrenTypes.push_back(child->getDataType().copy());
            } else {
                ExpressionUtil::validateExpressionType(*child,
                    {ExpressionType::LITERAL, ExpressionType::PARAMETER});
                child->setAlias(functionExpr->getChild(i)->getAlias());
                optionalParams.push_back(child);
            }
        }
        auto func = BuiltInFunctionsUtils::matchFunction(functionName, childrenTypes,
            entry->ptrCast<FunctionCatalogEntry>());
        auto gdsFunc = func->constPtrCast<GDSFunction>()->copy();
        auto input = GDSBindInput();
        input.params = children;
        input.binder = this;
        input.optionalParams = std::move(optionalParams);
        input.yieldVariables = call.getYieldVariables();
        gdsFunc.gds->bind(input, *clientContext);
        columns = gdsFunc.gds->getResultColumns(input);
        auto info = BoundGDSCallInfo(gdsFunc.copy(), std::move(columns));
        boundReadingClause = std::make_unique<BoundGDSCall>(std::move(info));
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
