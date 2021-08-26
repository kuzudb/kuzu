#include "src/binder/include/bound_statements/bound_match_statement.h"

namespace graphflow {
namespace binder {

void BoundMatchStatement::merge(BoundMatchStatement& other) {
    queryGraph->merge(*other.queryGraph);
    if (other.hasWhereExpression()) {
        whereExpression = hasWhereExpression() ? make_shared<Expression>(AND, BOOL, whereExpression,
                                                     other.whereExpression) :
                                                 other.whereExpression;
    }
}

vector<shared_ptr<Expression>> BoundMatchStatement::getDependentProperties() const {
    if (hasWhereExpression()) {
        return whereExpression->getDependentProperties();
    }
    return vector<shared_ptr<Expression>>();
}

} // namespace binder
} // namespace graphflow
