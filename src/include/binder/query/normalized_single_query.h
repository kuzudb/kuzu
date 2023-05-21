#pragma once

#include "binder/bound_statement_result.h"
#include "normalized_query_part.h"

namespace kuzu {
namespace binder {

class NormalizedSingleQuery {
public:
    NormalizedSingleQuery() = default;
    ~NormalizedSingleQuery() = default;

    inline void appendQueryPart(std::unique_ptr<NormalizedQueryPart> queryPart) {
        queryParts.push_back(std::move(queryPart));
    }
    inline uint32_t getNumQueryParts() const { return queryParts.size(); }
    inline NormalizedQueryPart* getQueryPart(uint32_t idx) const { return queryParts[idx].get(); }

private:
    std::vector<std::unique_ptr<NormalizedQueryPart>> queryParts;
};

} // namespace binder
} // namespace kuzu
