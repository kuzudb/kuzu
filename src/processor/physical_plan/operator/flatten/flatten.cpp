#include "src/processor/include/physical_plan/operator/flatten/flatten.h"

namespace graphflow {
namespace processor {

void Flatten::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    PhysicalOperator::initResultSet(resultSet);
    dataChunkToFlatten = resultSet->dataChunks[dataChunkToFlattenPos];
}

bool Flatten::getNextTuples() {
    metrics->executionTime.start();
    // currentIdx == -1 is the check for initial case
    if (dataChunkToFlatten->state->currIdx == -1 ||
        dataChunkToFlatten->state->selectedSize == dataChunkToFlatten->state->currIdx + 1ul) {
        dataChunkToFlatten->state->currIdx = -1;
        if (!prevOperator->getNextTuples()) {
            metrics->executionTime.stop();
            return false;
        }
    }
    dataChunkToFlatten->state->currIdx++;
    metrics->executionTime.stop();
    metrics->numOutputTuple.incrementByOne();
    return true;
}

} // namespace processor
} // namespace graphflow
