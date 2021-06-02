#pragma once

#include <cstdint>

#include "src/processor/include/physical_plan/operator/result/tuple.h"

namespace graphflow {
namespace processor {

class QueryResult {

public:
    explicit QueryResult(uint64_t numTuples) : numTuples{numTuples} {}

    explicit QueryResult(vector<Tuple> tuples) : numTuples{tuples.size()}, tuples{move(tuples)} {}

    QueryResult() : QueryResult(0) {}

    void appendQueryResult(unique_ptr<QueryResult> result) {
        numTuples += result->numTuples;
        if constexpr (ENABLE_DEBUG) {
            move(begin(result->tuples), end(result->tuples), back_inserter(tuples));
        }
    }

public:
    uint64_t numTuples;
    vector<Tuple> tuples;
};

} // namespace processor
} // namespace graphflow
