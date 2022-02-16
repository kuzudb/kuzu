#include "include/bound_projection_body.h"

namespace graphflow {
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
    vector<shared_ptr<Expression>> expressions, vector<bool> sortOrders) {
    orderByExpressions = move(expressions);
    isAscOrders = move(sortOrders);
}

} // namespace binder
} // namespace graphflow
