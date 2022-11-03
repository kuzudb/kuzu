#pragma once

#include "src/binder/query/include/bound_single_query.h"
#include "src/binder/query/include/normalized_single_query.h"

namespace graphflow {
namespace binder {

// Rewriter performs a sequence of rewrites which do not change the semantic but simplifies
// planning and processing later on.
class Rewriter {
public:
    static unique_ptr<NormalizedSingleQuery> rewrite(const BoundSingleQuery& singleQuery);
};

} // namespace binder
} // namespace graphflow
