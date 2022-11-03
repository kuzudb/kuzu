#pragma once

#include "src/binder/query/include/bound_single_query.h"
#include "src/binder/query/include/normalized_single_query.h"

namespace graphflow {
namespace binder {

class BoundSingleQueryRewriter {
public:
    // BoundSingleQuery contains (BoundQueryPart)* and a final "MATCH ... UPDATE ... RETURN ...".
    // We rewrite as (BoundQueryPart)+
    static unique_ptr<NormalizedSingleQuery> rewrite(const BoundSingleQuery& singleQuery);

private:
    // Rewrite final "MATCH ... UPDATE ... RETURN ..." as a BoundQueryPart.
    static unique_ptr<BoundQueryPart> rewriteFinalQueryPart(const BoundSingleQuery& singleQuery);

    // Extract projection body and projection predicate out of BoundQueryPart.
    static unique_ptr<NormalizedQueryPart> rewriteQueryPart(const BoundQueryPart& queryPart);
};

} // namespace binder
} // namespace graphflow