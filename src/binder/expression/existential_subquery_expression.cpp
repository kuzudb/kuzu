#include "src/binder/include/expression/existential_subquery_expression.h"

namespace graphflow {
namespace binder {

// NOTE:
vector<const Expression*> ExistentialSubqueryExpression::getIncludedExpressionsWithTypes(
    const unordered_set<ExpressionType>& expressionTypes) const {
    vector<const Expression*> expressions;
    if (expressionTypes.contains(VARIABLE)) {
        for (auto& expression : subquery->getIncludedVariables()) {
            expressions.push_back(expression);
        }
    }
    return expressions;
}

} // namespace binder
} // namespace graphflow