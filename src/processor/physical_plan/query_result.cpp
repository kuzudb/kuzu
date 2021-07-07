#include "src/processor/include/physical_plan/query_result.h"

namespace graphflow {
namespace processor {

void QueryResult::appendQueryResult(unique_ptr<QueryResult> queryResult) {
    numTuples += queryResult->numTuples;
    move(begin(queryResult->resultSetCollection), end(queryResult->resultSetCollection),
        back_inserter(resultSetCollection));
    move(begin(queryResult->bufferBlocks), end(queryResult->bufferBlocks),
        back_inserter(bufferBlocks));
}
} // namespace processor
} // namespace graphflow