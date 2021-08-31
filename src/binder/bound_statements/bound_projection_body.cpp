#include "src/binder/include/bound_statements/bound_projection_body.h"

namespace graphflow {
namespace binder {

vector<shared_ptr<Expression>> BoundProjectionBody::getDependentProperties() const {
    vector<shared_ptr<Expression>> result;
    for (auto& projectionExpression : projectionExpressions) {
        for (auto& propertyExpression : projectionExpression->getDependentProperties()) {
            result.push_back(propertyExpression);
        }
    }
    return result;
}

} // namespace binder
} // namespace graphflow
