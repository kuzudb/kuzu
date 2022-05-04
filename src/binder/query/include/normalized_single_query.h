#pragma once

#include "normalized_query_part.h"

namespace graphflow {
namespace binder {

/**
 * See QueryNormalizer for detailed normalization performed.
 */
class NormalizedSingleQuery {

public:
    NormalizedSingleQuery() = default;

    ~NormalizedSingleQuery() = default;

    inline void appendQueryPart(unique_ptr<NormalizedQueryPart> queryPart) {
        queryParts.push_back(move(queryPart));
    }
    inline uint32_t getNumQueryParts() const { return queryParts.size(); }
    inline NormalizedQueryPart* getQueryPart(uint32_t idx) const { return queryParts[idx].get(); }
    inline NormalizedQueryPart* getLastQueryPart() const {
        return queryParts[queryParts.size() - 1].get();
    }

    inline expression_vector getExpressionsToReturn() const {
        assert(!queryParts.empty());
        return queryParts.back()->getProjectionBody()->getProjectionExpressions();
    }

    expression_vector getPropertiesToRead() const;

private:
    vector<unique_ptr<NormalizedQueryPart>> queryParts;
};

} // namespace binder
} // namespace graphflow
