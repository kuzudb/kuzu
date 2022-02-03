#pragma once

#include "src/binder/include/bound_queries/bound_single_query.h"

namespace graphflow {
namespace binder {

class BoundRegularQuery {

public:
    inline void addBoundSingleQuery(unique_ptr<BoundSingleQuery> boundSingleQuery) {
        boundSingleQueries.push_back(move(boundSingleQuery));
    }

    inline BoundSingleQuery* getBoundSingleQuery(uint32_t idx) const {
        return boundSingleQueries[idx].get();
    }

    inline uint64_t getNumBoundSingleQueries() const { return boundSingleQueries.size(); }

private:
    vector<unique_ptr<BoundSingleQuery>> boundSingleQueries;
};

} // namespace binder
} // namespace graphflow
