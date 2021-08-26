#include "src/binder/include/bound_statements/bound_with_statement.h"

namespace graphflow {
namespace binder {

vector<shared_ptr<Expression>> BoundWithStatement::getDependentProperties() const {
    auto result = BoundReturnStatement::getDependentProperties();
    if (hasWhereExpression()) {
        for (auto& propertyExpression : whereExpression->getDependentProperties()) {
            result.push_back(propertyExpression);
        }
    }
    return result;
}

} // namespace binder
} // namespace graphflow
