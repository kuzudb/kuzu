#include "include/normalized_query_part.h"

namespace graphflow {
namespace binder {

void NormalizedQueryPart::addQueryGraph(unique_ptr<QueryGraph> queryGraph,
    shared_ptr<Expression> predicate, bool isQueryGraphOptional) {
    queryGraphs.push_back(move(queryGraph));
    queryGraphPredicates.push_back(move(predicate));
    isOptional.push_back(isQueryGraphOptional);
}

expression_vector NormalizedQueryPart::getPropertiesToRead() const {
    expression_vector result;
    for (auto i = 0u; i < getNumQueryGraph(); ++i) {
        if (hasQueryGraphPredicate(i)) {
            for (auto& property : queryGraphPredicates[i]->getSubPropertyExpressions()) {
                result.push_back(property);
            }
        }
    }
    for (auto& setClause : setClauses) {
        for (auto& property : setClause->getPropertiesToRead()) {
            result.push_back(property);
        }
    }
    if (hasProjectionBody()) {
        for (auto& property : projectionBody->getPropertiesToRead()) {
            result.push_back(property);
        }
        if (hasProjectionBodyPredicate()) {
            for (auto& property : projectionBodyPredicate->getSubPropertyExpressions()) {
                result.push_back(property);
            }
        }
    }
    return result;
}

} // namespace binder
} // namespace graphflow
