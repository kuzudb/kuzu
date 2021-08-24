#include "src/binder/include/expression/existential_subquery_expression.h"

namespace graphflow {
namespace binder {

// NOTE:
vector<shared_ptr<Expression>> ExistentialSubqueryExpression::getIncludedExpressionsWithTypes(
    const unordered_set<ExpressionType>& expressionTypes) {
    vector<shared_ptr<Expression>> expressions;
    if (expressionTypes.contains(VARIABLE)) {
        for (auto& expression : subquery->getIncludedVariables()) {
            expressions.push_back(expression);
        }
    }
    if (expressionTypes.contains(EXISTENTIAL_SUBQUERY)) {
        expressions.push_back(shared_from_this());
    }
    return expressions;
}

} // namespace binder
} // namespace graphflow