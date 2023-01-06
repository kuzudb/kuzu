#pragma once

#include "binder/bound_statement.h"
#include "normalized_single_query.h"

namespace kuzu {
namespace binder {

class BoundRegularQuery : public BoundStatement {
public:
    explicit BoundRegularQuery(
        vector<bool> isUnionAll, unique_ptr<BoundStatementResult> statementResult)
        : BoundStatement{StatementType::QUERY, std::move(statementResult)}, isUnionAll{std::move(
                                                                                isUnionAll)} {}

    ~BoundRegularQuery() = default;

    inline bool isReadOnly() const override {
        for (auto& singleQuery : singleQueries) {
            if (!singleQuery->isReadOnly()) {
                return false;
            }
        }
        return true;
    }

    inline void addSingleQuery(unique_ptr<NormalizedSingleQuery> singleQuery) {
        singleQueries.push_back(std::move(singleQuery));
    }
    inline uint64_t getNumSingleQueries() const { return singleQueries.size(); }
    inline NormalizedSingleQuery* getSingleQuery(uint32_t idx) const {
        return singleQueries[idx].get();
    }

    inline bool getIsUnionAll(uint32_t idx) const { return isUnionAll[idx]; }

private:
    vector<unique_ptr<NormalizedSingleQuery>> singleQueries;
    vector<bool> isUnionAll;
};

} // namespace binder
} // namespace kuzu
