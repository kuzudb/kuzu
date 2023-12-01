#pragma once

#include "binder/bound_statement.h"
#include "normalized_single_query.h"

namespace kuzu {
namespace binder {

class BoundRegularQuery : public BoundStatement {
public:
    explicit BoundRegularQuery(
        std::vector<bool> isUnionAll, std::unique_ptr<BoundStatementResult> statementResult)
        : BoundStatement{common::StatementType::QUERY, std::move(statementResult)},
          isUnionAll{std::move(isUnionAll)} {}

    ~BoundRegularQuery() override = default;

    inline void addSingleQuery(std::unique_ptr<NormalizedSingleQuery> singleQuery) {
        singleQueries.push_back(std::move(singleQuery));
    }
    inline uint64_t getNumSingleQueries() const { return singleQueries.size(); }
    inline NormalizedSingleQuery* getSingleQuery(uint32_t idx) const {
        return singleQueries[idx].get();
    }

    inline bool getIsUnionAll(uint32_t idx) const { return isUnionAll[idx]; }

private:
    std::vector<std::unique_ptr<NormalizedSingleQuery>> singleQueries;
    std::vector<bool> isUnionAll;
};

} // namespace binder
} // namespace kuzu
