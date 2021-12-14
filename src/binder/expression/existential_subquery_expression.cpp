#include "src/binder/include/expression/existential_subquery_expression.h"

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
    for (auto& queryPart : normalizedSubquery->getQueryParts()) {
        for (auto i = 0u; i < queryPart->getNumQueryGraph(); ++i) {
            for (auto& nodeIDExpression : queryPart->getQueryGraph(i)->getNodeIDExpressions()) {
                result.push_back(nodeIDExpression);
            }
            if (queryPart->getQueryGraphPredicate(i)) {
                result.push_back(queryPart->getQueryGraphPredicate(i));
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
