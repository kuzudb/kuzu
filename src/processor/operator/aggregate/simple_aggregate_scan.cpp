#include "processor/operator/aggregate/simple_aggregate_scan.h"

namespace kuzu {
namespace processor {

bool SimpleAggregateScan::getNextTuplesInternal() {
    auto [startOffset, endOffset] = sharedState->getNextRangeToRead();
    if (startOffset >= endOffset) {
        return false;
    }
    // Output of simple aggregate is guaranteed to be a single value for each aggregate.
    assert(startOffset == 0);
    assert(endOffset == 1);
    for (auto i = 0u; i < aggregateVectors.size(); i++) {
        writeAggregateResultToVector(
            *aggregateVectors[i], 0 /* position to write */, sharedState->getAggregateState(i));
    }
    assert(!aggregatesPos.empty());
    auto outDataChunk = resultSet->dataChunks[aggregatesPos[0].dataChunkPos];
    outDataChunk->state->initOriginalAndSelectedSize(1);
    metrics->numOutputTuple.increase(outDataChunk->state->selVector->selectedSize);
    return true;
}

} // namespace processor
} // namespace kuzu
