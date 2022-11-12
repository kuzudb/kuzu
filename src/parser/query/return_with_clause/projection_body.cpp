#include "include/projection_body.h"

namespace kuzu {
namespace parser {

bool ProjectionBody::operator==(const ProjectionBody& other) const {
    if (isDistinct != other.isDistinct || containsStar != other.containsStar ||
        projectionExpressions.size() != other.projectionExpressions.size() ||
        orderByExpressions.size() != other.orderByExpressions.size() ||
        hasSkipExpression() != other.hasSkipExpression() ||
        hasLimitExpression() != other.hasLimitExpression()) {
        return false;
    }
    for (auto i = 0u; i < projectionExpressions.size(); ++i) {
        if (*projectionExpressions[i] != *other.projectionExpressions[i]) {
            return false;
        }
    }
    for (auto i = 0u; i < orderByExpressions.size(); ++i) {
        if (*orderByExpressions[i] != *other.orderByExpressions[i] ||
            isAscOrders[i] != other.isAscOrders[i]) {
            return false;
        }
    }
    if (hasSkipExpression() && *skipExpression != *other.skipExpression) {
        return false;
    }
    if (hasLimitExpression() && *limitExpression != *other.limitExpression) {
        return false;
    }
    return true;
}

} // namespace parser
} // namespace kuzu
