#include "include/rewriter.h"

#include "rewriter_rules/include/bound_single_query_rewriter.h"

namespace graphflow {
namespace binder {

unique_ptr<NormalizedSingleQuery> Rewriter::rewrite(const BoundSingleQuery& singleQuery) {
    auto result = BoundSingleQueryRewriter::rewrite(singleQuery);

    // CTE rewriter
    // If former query part (CTE) does not contain UPDATE, AGG, ORDER BY, SKIP and LIMIT, we merge
    // former and latter query parts into one.
    // E.g. "MATCH (a) WITH a.age AS newAge MATCH (a)->(b) WHERE b.age = newAge RETURN ..."
    // will be rewritten as "MATCH (a)->(b) WHERE b.age = a.age RETURN ..."
    // This will expand our plan space.

    // Disconnected query graph rewriter
    // Rewrite disconnected graph pattern as multiple query part, each of which contains a single
    // MATCH clause with connected graph pattern.
    // This will simplify our planning.

    return result;
}

} // namespace binder
} // namespace graphflow
