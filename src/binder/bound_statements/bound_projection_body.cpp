#include "src/binder/include/bound_statements/bound_projection_body.h"

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

vector<shared_ptr<Expression>> BoundProjectionBody::getAggregationExpressions() const {
    vector<shared_ptr<Expression>> aggregationExpressions;
    for (auto& projectionExpression : projectionExpressions) {
        for (auto& aggregationExpression :
            projectionExpression->getTopLevelSubAggregationExpressions()) {
            aggregationExpressions.push_back(aggregationExpression);
        }
    }
    return aggregationExpressions;
}

} // namespace binder
} // namespace graphflow
