#pragma once

#include "binder/bound_statement.h"
#include "normalized_single_query.h"

namespace kuzu {
namespace binder {

class BoundRegularQuery : public BoundStatement {
public:
    explicit BoundRegularQuery(std::vector<bool> isUnionAll, BoundStatementResult statementResult)
        : BoundStatement{common::StatementType::QUERY, std::move(statementResult)},
          isUnionAll{std::move(isUnionAll)} {}

    inline void addSingleQuery(NormalizedSingleQuery singleQuery) {
        singleQueries.push_back(std::move(singleQuery));
    }
    inline uint64_t getNumSingleQueries() const { return singleQueries.size(); }
    inline NormalizedSingleQuery* getSingleQueryUnsafe(uint32_t idx) { return &singleQueries[idx]; }
    inline const NormalizedSingleQuery* getSingleQuery(uint32_t idx) const {
        return &singleQueries[idx];
    }

    inline bool getIsUnionAll(uint32_t idx) const { return isUnionAll[idx]; }

private:
    std::vector<NormalizedSingleQuery> singleQueries;
    std::vector<bool> isUnionAll;
};

} // namespace binder
} // namespace kuzu
