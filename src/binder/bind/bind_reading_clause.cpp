#include "binder/binder.h"
#include "binder/call/bound_in_query_call.h"
#include "binder/expression/literal_expression.h"
#include "binder/query/reading_clause/bound_unwind_clause.h"
#include "parser/call/in_query_call.h"
#include "parser/query/reading_clause/unwind_clause.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundReadingClause> Binder::bindReadingClause(const ReadingClause& readingClause) {
    switch (readingClause.getClauseType()) {
    case ClauseType::MATCH: {
        return bindMatchClause((MatchClause&)readingClause);
    }
    case ClauseType::UNWIND: {
        return bindUnwindClause((UnwindClause&)readingClause);
    }
    case ClauseType::InQueryCall: {
        return bindInQueryCall((InQueryCall&)readingClause);
    }
    default:
        throw NotImplementedException("bindReadingClause().");
    }
}

std::unique_ptr<BoundReadingClause> Binder::bindMatchClause(const ReadingClause& readingClause) {
    auto& matchClause = reinterpret_cast<const MatchClause&>(readingClause);
    auto [queryGraphCollection, propertyCollection] =
        bindGraphPattern(matchClause.getPatternElements());
    auto boundMatchClause =
        make_unique<BoundMatchClause>(std::move(queryGraphCollection), matchClause.getIsOptional());
    std::shared_ptr<Expression> whereExpression;
    if (matchClause.hasWhereClause()) {
        whereExpression = bindWhereExpression(*matchClause.getWhereClause());
    }
    // Rewrite key value pairs in MATCH clause as predicate
    for (auto& [key, val] : propertyCollection->getKeyVals()) {
        auto predicate = expressionBinder.createEqualityComparisonExpression(key, val);
        whereExpression =
            expressionBinder.combineConjunctiveExpressions(predicate, whereExpression);
    }
    boundMatchClause->setWhereExpression(std::move(whereExpression));
    return boundMatchClause;
}

std::unique_ptr<BoundReadingClause> Binder::bindUnwindClause(const ReadingClause& readingClause) {
    auto& unwindClause = reinterpret_cast<const UnwindClause&>(readingClause);
    auto boundExpression = expressionBinder.bindExpression(*unwindClause.getExpression());
    boundExpression =
        ExpressionBinder::implicitCastIfNecessary(boundExpression, LogicalTypeID::VAR_LIST);
    auto aliasExpression = createVariable(
        unwindClause.getAlias(), *common::VarListType::getChildType(&boundExpression->dataType));
    return make_unique<BoundUnwindClause>(std::move(boundExpression), std::move(aliasExpression));
}

std::unique_ptr<BoundReadingClause> Binder::bindInQueryCall(const ReadingClause& readingClause) {
    auto& callStatement = reinterpret_cast<const parser::InQueryCall&>(readingClause);
    auto tableFunctionDefinition =
        catalog.getBuiltInTableFunction()->mathTableFunction(callStatement.getFuncName());
    auto boundExpr = expressionBinder.bindLiteralExpression(*callStatement.getParameter());
    auto inputValue = reinterpret_cast<LiteralExpression*>(boundExpr.get())->getValue();
    auto bindData = tableFunctionDefinition->bindFunc(clientContext,
        function::TableFuncBindInput{std::vector<common::Value>{*inputValue}},
        catalog.getReadOnlyVersion());
    expression_vector outputExpressions;
    for (auto i = 0u; i < bindData->returnColumnNames.size(); i++) {
        outputExpressions.push_back(
            createVariable(bindData->returnColumnNames[i], bindData->returnTypes[i]));
    }
    return std::make_unique<BoundInQueryCall>(
        tableFunctionDefinition->tableFunc, std::move(bindData), std::move(outputExpressions));
}

} // namespace binder
} // namespace kuzu
