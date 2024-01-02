#pragma once

#include "binder/bound_statement_result.h"
#include "normalized_query_part.h"

namespace kuzu {
namespace binder {

class NormalizedSingleQuery {
public:
    NormalizedSingleQuery() = default;
    DELETE_COPY_DEFAULT_MOVE(NormalizedSingleQuery);

    inline void appendQueryPart(NormalizedQueryPart queryPart) {
        queryParts.push_back(std::move(queryPart));
    }
    inline uint32_t getNumQueryParts() const { return queryParts.size(); }
    inline NormalizedQueryPart* getQueryPartUnsafe(uint32_t idx) { return &queryParts[idx]; }
    inline const NormalizedQueryPart* getQueryPart(uint32_t idx) const { return &queryParts[idx]; }

    inline void setStatementResult(BoundStatementResult result) {
        statementResult = std::move(result);
    }
    inline const BoundStatementResult* getStatementResult() const { return &statementResult; }

private:
    std::vector<NormalizedQueryPart> queryParts;
    BoundStatementResult statementResult;
};

} // namespace binder
} // namespace kuzu
