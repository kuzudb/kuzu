#include "src/processor/include/physical_plan/operator/sink/result_collector.h"

namespace graphflow {
namespace processor {

void ResultCollector::getNextTuples() {
    prevOperator->getNextTuples();
    queryResult->numTuples += prevOperator->getDataChunks()->getNumTuples();
    if constexpr (ENABLE_DEBUG) {
        dataChunksIterator->reset();
        while (dataChunksIterator->hasNextTuple()) {
            Tuple tuple(dataChunksIterator->dataChunksTypes);
            dataChunksIterator->getNextTuple(tuple);
            queryResult->tuples.push_back(move(tuple));
        }
    }
}

} // namespace processor
} // namespace graphflow
