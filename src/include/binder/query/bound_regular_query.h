#pragma once

#include "binder/bound_statement.h"
#include "normalized_single_query.h"

namespace kuzu {
namespace binder {

class BoundRegularQuery final : public BoundStatement {

public:
    explicit BoundRegularQuery(std::vector<bool> isUnionAll, BoundStatementResult statementResult)
        : BoundStatement{common::StatementType::QUERY, std::move(statementResult)},
          isUnionAll{std::move(isUnionAll)} {}

    void addSingleQuery(NormalizedSingleQuery singleQuery) {
        singleQueries.push_back(std::move(singleQuery));
    }
    uint64_t getNumSingleQueries() const { return singleQueries.size(); }
    NormalizedSingleQuery* getSingleQueryUnsafe(common::idx_t idx) { return &singleQueries[idx]; }
    const NormalizedSingleQuery* getSingleQuery(common::idx_t idx) const {
        return &singleQueries[idx];
    }

    bool getIsUnionAll(common::idx_t idx) const { return isUnionAll[idx]; }

private:
    std::vector<NormalizedSingleQuery> singleQueries;
    std::vector<bool> isUnionAll;
};

} // namespace binder
} // namespace kuzu
