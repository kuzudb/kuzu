#pragma once

#include "src/planner/include/norm_query/normalized_query_part.h"

namespace graphflow {
namespace planner {

/**
 * See QueryNormalizer for detailed normalization performed.
 */
class NormalizedQuery {

public:
    inline NormalizedQueryPart* getQueryPart(uint32_t idx) const { return queryParts[idx].get(); }
    inline const vector<unique_ptr<NormalizedQueryPart>>& getQueryParts() const {
        return queryParts;
    }
    inline void appendQueryPart(unique_ptr<NormalizedQueryPart> queryPart) {
        queryParts.push_back(move(queryPart));
    }

private:
    vector<unique_ptr<NormalizedQueryPart>> queryParts;
};

} // namespace planner
} // namespace graphflow
