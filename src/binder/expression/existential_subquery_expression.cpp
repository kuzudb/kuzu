#include "src/binder/include/expression/existential_subquery_expression.h"

namespace graphflow {
namespace binder {

vector<shared_ptr<Expression>> ExistentialSubqueryExpression::getDependentVariables() {
    auto& firstQueryPart = *normalizedSubquery->getQueryPart(0);
    vector<shared_ptr<Expression>> result;
    for (auto& node : firstQueryPart.getQueryGraph()->queryNodes) {
        result.push_back(node);
    }
    for (auto& rel : firstQueryPart.getQueryGraph()->queryRels) {
        result.push_back(rel);
    }
    if (firstQueryPart.hasWhereExpression()) {
        for (auto& variable : firstQueryPart.getWhereExpression()->getDependentVariables()) {
            result.push_back(variable);
        }
    }
    for (auto& projectExpression : firstQueryPart.getProjectionBody()->getProjectionExpressions()) {
        for (auto& variable : projectExpression->getDependentVariables()) {
            result.push_back(variable);
        }
    }
    return result;
}

vector<shared_ptr<Expression>> ExistentialSubqueryExpression::getDependentProperties() {
    auto& firstQueryPart = *normalizedSubquery->getQueryPart(0);
    vector<shared_ptr<Expression>> result;
    if (firstQueryPart.hasWhereExpression()) {
        for (auto& variable : firstQueryPart.getWhereExpression()->getDependentProperties()) {
            result.push_back(variable);
        }
    }
    for (auto& projectExpression : firstQueryPart.getProjectionBody()->getProjectionExpressions()) {
        for (auto& variable : projectExpression->getDependentProperties()) {
            result.push_back(variable);
        }
    }
    return result;
}

vector<shared_ptr<Expression>> ExistentialSubqueryExpression::getDependentLeafExpressions() {
    auto& firstQueryPart = *normalizedSubquery->getQueryPart(0);
    auto result = firstQueryPart.getDependentNodeID();
    if (firstQueryPart.hasWhereExpression()) {
        for (auto& variable : firstQueryPart.getWhereExpression()->getDependentLeafExpressions()) {
            result.push_back(variable);
        }
    }
    for (auto& projectExpression : firstQueryPart.getProjectionBody()->getProjectionExpressions()) {
        for (auto& variable : projectExpression->getDependentLeafExpressions()) {
            result.push_back(variable);
        }
    }
    return result;
}

} // namespace binder
} // namespace graphflow
