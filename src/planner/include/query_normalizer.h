#pragma once

#include "src/binder/include/bound_queries/bound_single_query.h"
#include "src/planner/include/norm_query/normalized_query.h"

namespace graphflow {
namespace planner {

class QueryNormalizer {

public:
    static unique_ptr<NormalizedQuery> normalizeQuery(const BoundSingleQuery& boundSingleQuery);

private:
    static unique_ptr<BoundQueryPart> normalizeFinalReadsAndReturnAsQueryPart(
        const BoundSingleQuery& boundSingleQuery);

    static unique_ptr<NormalizedQueryPart> normalizeQueryPart(const BoundQueryPart& boundQueryPart);

    // Recursively normalize bound query within subquery expression
    static void normalizeSubqueryExpression(const BoundQueryPart& boundQueryPart);
};

} // namespace planner
} // namespace graphflow
