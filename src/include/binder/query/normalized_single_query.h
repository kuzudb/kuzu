#pragma once

#include "binder/bound_statement_result.h"
#include "normalized_query_part.h"

namespace kuzu {
namespace binder {

class NormalizedSingleQuery {
public:
    inline void appendQueryPart(std::unique_ptr<NormalizedQueryPart> queryPart) {
        queryParts.push_back(std::move(queryPart));
    }
    inline uint32_t getNumQueryParts() const { return queryParts.size(); }
    inline NormalizedQueryPart* getQueryPart(uint32_t idx) const { return queryParts[idx].get(); }

    inline void setStatementResult(std::unique_ptr<BoundStatementResult> result) {
        statementResult = std::move(result);
    }
    inline BoundStatementResult* getStatementResult() const { return statementResult.get(); }

private:
    std::vector<std::unique_ptr<NormalizedQueryPart>> queryParts;
    std::unique_ptr<BoundStatementResult> statementResult;
};

} // namespace binder
} // namespace kuzu
