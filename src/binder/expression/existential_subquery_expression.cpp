#include "src/binder/include/expression/existential_subquery_expression.h"

namespace graphflow {
namespace binder {

vector<shared_ptr<Expression>> ExistentialSubqueryExpression::getSubVariableExpressions() {
    auto& firstQueryPart = *normalizedSubquery->getQueryPart(0);
    vector<shared_ptr<Expression>> result;
    for (auto& node : firstQueryPart.getQueryGraph()->queryNodes) {
        result.push_back(node);
    }
    for (auto& rel : firstQueryPart.getQueryGraph()->queryRels) {
        result.push_back(rel);
    }
    for (auto& expression : getSubExpressions()) {
        for (auto& variable : expression->getSubVariableExpressions()) {
            result.push_back(variable);
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
