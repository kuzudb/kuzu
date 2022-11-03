#pragma once

#include "src/binder/query/include/bound_single_query.h"
#include "src/binder/query/include/normalized_single_query.h"

namespace graphflow {
namespace binder {

// BoundSingleQueryRewriter performs 2 rewrites
// 1. Rewrite "(MATCH UPDATE WITH)* MATCH UPDATE RETURN" as "(MATCH UPDATE WITH)+"
// 2. Rewrite consecutive MATCH clauses as one MATCH clause, e.g. MATCH ... MATCH ...
class BoundSingleQueryRewriter {
public:
    static unique_ptr<NormalizedSingleQuery> rewrite(const BoundSingleQuery& singleQuery);

private:
    // Rewrite final "MATCH ... UPDATE ... RETURN ..." as a BoundQueryPart.
    static unique_ptr<BoundQueryPart> rewriteFinalQueryPart(const BoundSingleQuery& singleQuery);

    // Rewrite consecutive MATCH clauses as one MATCH clause
    static unique_ptr<NormalizedQueryPart> rewriteQueryPart(const BoundQueryPart& queryPart);

    static bool canMergeReadingClause(
        const BoundReadingClause& former, const BoundReadingClause& latter);
    static void mergeReadingClause(BoundReadingClause& former, const BoundReadingClause& latter);
};

} // namespace binder
} // namespace graphflow