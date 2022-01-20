#pragma once

#include <cstdint>

#include "src/processor/include/physical_plan/result/flat_tuple.h"
#include "src/processor/include/physical_plan/result/result_set.h"

namespace graphflow {
namespace processor {

class QueryResult {

public:
    QueryResult(vector<DataPos> vectorsToCollectPos, unordered_set<uint32_t> dataChunksPosInScope)
        : numTuples{0}, vectorsToCollectPos{move(vectorsToCollectPos)}, dataChunksPosInScope{move(
                                                                            dataChunksPosInScope)} {
    }

    void appendQueryResult(unique_ptr<QueryResult> queryResult);

    void clear();

public:
    uint64_t numTuples;
    vector<DataPos> vectorsToCollectPos;
    unordered_set<uint32_t> dataChunksPosInScope;
    vector<unique_ptr<ResultSet>> resultSetCollection;
    vector<unique_ptr<BufferBlock>> bufferBlocks;
};

} // namespace processor
} // namespace graphflow
