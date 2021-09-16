#include "src/binder/include/bound_statements/bound_projection_body.h"

namespace graphflow {
namespace binder {

bool BoundProjectionBody::hasAggregationExpressions() const {
    for (auto& projectionExpression : projectionExpressions) {
        if (isExpressionAggregate(projectionExpression->expressionType)) {
            return true;
        }
    }
    return false;
}

vector<shared_ptr<Expression>> BoundProjectionBody::getAggregationExpressions() const {
    vector<shared_ptr<Expression>> aggregationExpressions;
    for (auto& projectionExpression : projectionExpressions) {
        if (isExpressionAggregate(projectionExpression->expressionType)) {
            aggregationExpressions.push_back(projectionExpression);
        }
    }
    return aggregationExpressions;
}

} // namespace binder
} // namespace graphflow
