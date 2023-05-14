#include "binder/binder.h"
#include "binder/expression/existential_subquery_expression.h"
#include "binder/expression_binder.h"
#include "parser/expression/parsed_subquery_expression.h"

using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindExistentialSubqueryExpression(
    const ParsedExpression& parsedExpression) {
    auto& subqueryExpression = (ParsedSubqueryExpression&)parsedExpression;
    auto prevVariableScope = binder->enterSubquery();
    auto [queryGraph, _] = binder->bindGraphPattern(subqueryExpression.getPatternElements());
    auto rawName = parsedExpression.getRawName();
    auto uniqueName = binder->getUniqueExpressionName(rawName);
    auto boundSubqueryExpression = make_shared<ExistentialSubqueryExpression>(
        std::move(queryGraph), std::move(uniqueName), std::move(rawName));
    if (subqueryExpression.hasWhereClause()) {
        boundSubqueryExpression->setWhereExpression(
            binder->bindWhereExpression(*subqueryExpression.getWhereClause()));
    }
    binder->exitSubquery(std::move(prevVariableScope));
    return boundSubqueryExpression;
}

} // namespace binder
} // namespace kuzu
