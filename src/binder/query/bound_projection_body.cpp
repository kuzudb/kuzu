#include "binder/query/return_with_clause/bound_projection_body.h"

namespace kuzu {
namespace binder {

bool BoundProjectionBody::hasAggregationExpressions() const {
    for (auto& projectionExpression : projectionExpressions) {
        if (projectionExpression->hasAggregationExpression()) {
            return true;
        }
    }
    return false;
}

void BoundProjectionBody::setOrderByExpressions(
    expression_vector expressions, std::vector<bool> sortOrders) {
    orderByExpressions = std::move(expressions);
    isAscOrders = std::move(sortOrders);
}

expression_vector BoundProjectionBody::getPropertiesToRead() const {
    expression_vector result;
    for (auto& expression : projectionExpressions) {
        for (auto& property : expression->getSubPropertyExpressions()) {
            result.push_back(property);
        }
    }
    for (auto& expression : orderByExpressions) {
        for (auto& property : expression->getSubPropertyExpressions()) {
            result.push_back(property);
        }
    }
    return result;
}

} // namespace binder
} // namespace kuzu
