#pragma once

#include "src/binder/query/include/normalized_single_query.h"

namespace graphflow {
namespace binder {

class CTERewriter {
public:
    // If former query part (CTE) does not contain AGG, ORDER BY, SKIP and LIMIT, we merge former
    // and later query parts into one.
    // E.g. MATCH (a) WITH a.age AS newAge MATCH (a)->(b) WHERE b.age = newAge RETURN ...
    // will be rewritten as MATCH (a)->(b) WHERE b.age = a.age RETURN ...
    static unique_ptr<NormalizedSingleQuery> rewrite(const NormalizedSingleQuery& singleQuery);
};

} // namespace binder
} // namespace graphflow