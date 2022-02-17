#include "include/enumerator_utils.h"

namespace graphflow {
namespace planner {

property_vector EnumeratorUtils::getPropertiesToScan(const NormalizedSingleQuery& singleQuery) {
    vector<shared_ptr<PropertyExpression>> result;
    for (auto i = 0u; i < singleQuery.getNumQueryParts(); ++i) {
        auto queryPart = singleQuery.getQueryPart(i);
        for (auto j = 0u; j < queryPart->getNumQueryGraph(); ++j) {
            if (queryPart->hasQueryGraphPredicate(j)) {
                for (auto& property : getProperties(queryPart->getQueryGraphPredicate(j))) {
                    result.push_back(property);
                }
            }
        }
        for (auto& property : getPropertiesToScan(*queryPart->getProjectionBody())) {
            result.push_back(property);
        }
        if (queryPart->hasProjectionBodyPredicate()) {
            for (auto& property : getProperties(queryPart->getProjectionBodyPredicate())) {
                result.push_back(property);
            }
        }
    }
    return result;
}

property_vector EnumeratorUtils::getProperties(const shared_ptr<Expression>& expression) {
    vector<shared_ptr<PropertyExpression>> result;
    if (expression->expressionType == PROPERTY) {
        result.push_back(static_pointer_cast<PropertyExpression>(expression));
    }
    for (auto& child : expression->getChildren()) {
        for (auto& expr : getProperties(child)) {
            result.push_back(expr);
        }
    }
    return result;
}

property_vector EnumeratorUtils::getPropertiesForNode(
    const property_vector& propertiesToScan, const NodeExpression& node, bool isStructured) {
    property_vector result;
    for (auto& property : propertiesToScan) {
        if (property->getChild(0)->getUniqueName() == node.getUniqueName()) {
            assert(property->getChild(0)->dataType == NODE);
            if (isStructured == (property->dataType != UNSTRUCTURED)) {
                result.push_back(property);
            }
        }
    }
    return result;
}

property_vector EnumeratorUtils::getPropertiesForRel(
    const property_vector& propertiesToScan, const RelExpression& rel) {
    property_vector result;
    for (auto& property : propertiesToScan) {
        if (property->getChild(0)->getUniqueName() == rel.getUniqueName()) {
            assert(property->getChild(0)->dataType == REL);
            result.push_back(property);
        }
    }
    return result;
}

property_vector EnumeratorUtils::getPropertiesToScan(const BoundProjectionBody& projectionBody) {
    property_vector result;
    for (auto& expression : projectionBody.getProjectionExpressions()) {
        for (auto& property : getProperties(expression)) {
            result.push_back(property);
        }
    }
    for (auto& expression : projectionBody.getOrderByExpressions()) {
        for (auto& property : getProperties(expression)) {
            result.push_back(property);
        }
    }
    return result;
}

} // namespace planner
} // namespace graphflow
