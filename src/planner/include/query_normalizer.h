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

    static unique_ptr<NormalizedQueryPart> normalizeQueryPart(
        const BoundQueryPart& boundQueryPart, bool isFinalQueryPart);

    /**
     * Simply copy loadCSV clause to normalizedQueryPart
     */
    static void normalizeLoadCSVStatement(
        NormalizedQueryPart& normalizedQueryPart, const BoundQueryPart& boundQueryPart);

    /**
     * Merge multiple match clause as one query graph
     */
    static void normalizeQueryGraph(
        NormalizedQueryPart& normalizedQueryPart, const BoundQueryPart& boundQueryPart);

    /**
     * Merge where expression that exists in both match and with clause
     */
    static void normalizeWhereExpression(
        NormalizedQueryPart& normalizedQueryPart, const BoundQueryPart& boundQueryPart);

    static void normalizeSubqueryExpression(
        NormalizedQueryPart& normalizedQueryPart, const BoundQueryPart& boundQueryPart);
};

} // namespace planner
} // namespace graphflow
