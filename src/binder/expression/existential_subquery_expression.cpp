#include "src/binder/include/expression/existential_subquery_expression.h"

namespace graphflow {
namespace binder {

unordered_set<string> ExistentialSubqueryExpression::getDependentVariableNames() {
    unordered_set<string> result;
    for (auto& expression : getSubExpressions()) {
        for (auto& variableName : expression->getDependentVariableNames()) {
            result.insert(variableName);
        }
    }
    return result;
}

vector<shared_ptr<Expression>> ExistentialSubqueryExpression::getSubExpressions() {
    auto& firstQueryPart = *normalizedSubquery->getQueryPart(0);
    auto result = firstQueryPart.getNodeIDExpressions();
    if (firstQueryPart.hasWhereExpression()) {
        result.push_back(firstQueryPart.getWhereExpression());
    }
    for (auto& projectExpression : firstQueryPart.getProjectionBody()->getProjectionExpressions()) {
        result.push_back(projectExpression);
    }
    return result;
}

} // namespace binder
} // namespace graphflow
