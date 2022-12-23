#include "binder/expression/existential_subquery_expression.h"

namespace kuzu {
namespace binder {

// The children of subquery expressions is defined as all expressions in the subquery, i.e.
// expressions from predicates and return clause. Plus nodeID expressions from query graph.
expression_vector ExistentialSubqueryExpression::getChildren() const {
    expression_vector result;
    for (auto& node : queryGraphCollection->getQueryNodes()) {
        result.push_back(node->getInternalIDProperty());
    }
    if (hasWhereExpression()) {
        result.push_back(whereExpression);
    }
    return result;
}

} // namespace binder
} // namespace kuzu
