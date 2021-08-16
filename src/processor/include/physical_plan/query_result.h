#pragma once

#include <cstdint>

#include "src/processor/include/physical_plan/operator/result/result_set.h"
#include "src/processor/include/physical_plan/operator/result/tuple.h"

namespace graphflow {
namespace processor {

class QueryResult {

public:
    explicit QueryResult(uint64_t numTuples) : numTuples{numTuples} {}

    QueryResult() : QueryResult(0) {}

    void appendQueryResult(unique_ptr<QueryResult> queryResult);

    void clear();

public:
    uint64_t numTuples;
    vector<unique_ptr<ResultSet>> resultSetCollection;
    vector<unique_ptr<BufferBlock>> bufferBlocks;
};

} // namespace processor
} // namespace graphflow
