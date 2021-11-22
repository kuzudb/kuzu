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

} // namespace binder
} // namespace graphflow
