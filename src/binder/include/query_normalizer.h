#pragma once

#include "src/binder/query/include/bound_single_query.h"
#include "src/binder/query/include/normalized_single_query.h"

namespace graphflow {
namespace binder {

class QueryNormalizer {

public:
    static unique_ptr<NormalizedSingleQuery> normalizeQuery(const BoundSingleQuery& singleQuery);

private:
    static unique_ptr<BoundQueryPart> normalizeFinalMatchesAndReturnAsQueryPart(
        const BoundSingleQuery& singleQuery);

    static unique_ptr<NormalizedQueryPart> normalizeQueryPart(const BoundQueryPart& queryPart);
};

} // namespace binder
} // namespace graphflow
