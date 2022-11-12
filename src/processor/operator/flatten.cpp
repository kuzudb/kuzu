#include "include/flatten.h"

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> Flatten::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    dataChunkToFlatten = resultSet->dataChunks[dataChunkToFlattenPos];
    return resultSet;
}

bool Flatten::getNextTuples() {
    metrics->executionTime.start();
    // currentIdx == -1 is the check for initial case
    if (dataChunkToFlatten->state->currIdx == -1 || dataChunkToFlatten->state->isCurrIdxLast()) {
        dataChunkToFlatten->state->currIdx = -1;
        if (!children[0]->getNextTuples()) {
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
} // namespace kuzu
