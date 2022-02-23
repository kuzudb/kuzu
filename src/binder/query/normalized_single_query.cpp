#include "include/normalized_single_query.h"

namespace graphflow {
namespace binder {

expression_vector NormalizedSingleQuery::getAllPropertyExpressions() const {
    expression_vector result;
    for (auto i = 0u; i < getNumQueryParts(); ++i) {
        for (auto& property : getQueryPart(i)->getAllPropertyExpressions()) {
            result.push_back(property);
        }
    }
    return result;
}

} // namespace binder
} // namespace graphflow
