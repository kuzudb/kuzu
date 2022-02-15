#include "include/normalized_query_part.h"

namespace graphflow {
namespace binder {

void NormalizedQueryPart::addQueryGraph(unique_ptr<QueryGraph> queryGraph,
    shared_ptr<Expression> predicate, bool isQueryGraphOptional) {
    queryGraphs.push_back(move(queryGraph));
    queryGraphPredicates.push_back(move(predicate));
    isOptional.push_back(isQueryGraphOptional);
}

} // namespace binder
} // namespace graphflow
