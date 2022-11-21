#include "binder/query/normalized_single_query.h"

namespace kuzu {
namespace binder {

expression_vector NormalizedSingleQuery::getPropertiesToRead() const {
    expression_vector result;
    for (auto i = 0u; i < getNumQueryParts(); ++i) {
        for (auto& property : getQueryPart(i)->getPropertiesToRead()) {
            result.push_back(property);
        }
    }
    return result;
}

} // namespace binder
} // namespace kuzu
