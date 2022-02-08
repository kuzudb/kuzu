#pragma once

#include "src/binder/include/bound_queries/bound_single_query.h"

namespace graphflow {
namespace binder {

class BoundRegularQuery {

public:
    BoundRegularQuery(vector<bool> isUnionAll) : isUnionAll{isUnionAll} {}

    inline void addBoundSingleQuery(unique_ptr<BoundSingleQuery> boundSingleQuery) {
        boundSingleQueries.push_back(move(boundSingleQuery));
    }

    inline BoundSingleQuery* getBoundSingleQuery(uint32_t idx) const {
        return boundSingleQueries[idx].get();
    }

    inline uint64_t getNumBoundSingleQueries() const { return boundSingleQueries.size(); }

    inline bool getIsUnionAll(uint32_t idx) const { return isUnionAll[idx]; }

private:
    vector<unique_ptr<BoundSingleQuery>> boundSingleQueries;
    vector<bool> isUnionAll;
};

} // namespace binder
} // namespace graphflow
