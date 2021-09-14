#include "src/processor/include/physical_plan/operator/flatten/flatten.h"

namespace graphflow {
namespace processor {

void Flatten::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    PhysicalOperator::initResultSet(resultSet);
    dataChunkToFlatten = resultSet->dataChunks[dataChunkToFlattenPos];
}

void Flatten::getNextTuples() {
    metrics->executionTime.start();
    if (dataChunkToFlatten->state->selectedSize == 0ul ||
        dataChunkToFlatten->state->selectedSize == dataChunkToFlatten->state->currIdx + 1ul) {
        dataChunkToFlatten->state->currIdx = -1;
        prevOperator->getNextTuples();
        if (dataChunkToFlatten->state->selectedSize == 0) {
            metrics->executionTime.stop();
            return;
        }
    }
    dataChunkToFlatten->state->currIdx++;
    metrics->executionTime.stop();
    metrics->numOutputTuple.incrementByOne();
}

} // namespace processor
} // namespace graphflow
