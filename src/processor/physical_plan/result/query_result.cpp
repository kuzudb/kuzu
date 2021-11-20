#include "src/processor/include/physical_plan/result/query_result.h"

namespace graphflow {
namespace processor {

void QueryResult::appendQueryResult(unique_ptr<QueryResult> queryResult) {
    numTuples += queryResult->numTuples;
    move(begin(queryResult->resultSetCollection), end(queryResult->resultSetCollection),
        back_inserter(resultSetCollection));
    move(begin(queryResult->bufferBlocks), end(queryResult->bufferBlocks),
        back_inserter(bufferBlocks));
}

void QueryResult::clear() {
    numTuples = 0;
    resultSetCollection.clear();
    bufferBlocks.clear();
}

} // namespace processor
} // namespace graphflow
