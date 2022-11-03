#include "include/rewriter.h"

#include "rewriter_rules/include/bound_single_query_rewriter.h"

namespace graphflow {
namespace binder {

unique_ptr<NormalizedSingleQuery> Rewriter::rewrite(const BoundSingleQuery& singleQuery) {
    auto result = BoundSingleQueryRewriter::rewrite(singleQuery);

    return result;
}



} // namespace binder
} // namespace graphflow
