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

} // namespace binder
} // namespace kuzu
