#include "include/existential_subquery_expression.h"

namespace graphflow {
namespace binder {

unordered_set<string> ExistentialSubqueryExpression::getDependentVariableNames() {
    unordered_set<string> result;
    for (auto& expression : getChildren()) {
        for (auto& variableName : expression->getDependentVariableNames()) {
            result.insert(variableName);
        }
    }
    return result;
}

// The children of subquery expressions is defined as all expressions in the subquery, i.e.
// expressions from predicates and return clause. Plus nodeID expressions from query graph.
vector<shared_ptr<Expression>> ExistentialSubqueryExpression::getChildren() const {
    vector<shared_ptr<Expression>> result;
    for (auto i = 0u; i < subQuery->getNumQueryParts(); ++i) {
        auto queryPart = subQuery->getQueryPart(i);
        for (auto j = 0u; j < queryPart->getNumQueryGraph(); ++j) {
            for (auto& nodeIDExpression : queryPart->getQueryGraph(j)->getNodeIDExpressions()) {
                result.push_back(nodeIDExpression);
            }
            if (queryPart->hasQueryGraphPredicate(j)) {
                result.push_back(queryPart->getQueryGraphPredicate(j));
            }
        }
        for (auto& expression : queryPart->getProjectionBody()->getProjectionExpressions()) {
            result.push_back(expression);
        }
    }
    return result;
}

} // namespace binder
} // namespace graphflow
