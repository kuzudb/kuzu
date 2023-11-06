#include "processor/operator/aggregate/simple_aggregate_scan.h"

namespace kuzu {
namespace processor {

void SimpleAggregateScan::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    BaseAggregateScan::initLocalStateInternal(resultSet, context);
    assert(!aggregatesPos.empty());
    auto outDataChunkPos = aggregatesPos[0].dataChunkPos;
    for (auto& dataPos : aggregatesPos) {
        assert(dataPos.dataChunkPos == outDataChunkPos);
    }
    outDataChunk = resultSet->dataChunks[outDataChunkPos].get();
}

bool SimpleAggregateScan::getNextTuplesInternal(ExecutionContext* /*context*/) {
    auto [startOffset, endOffset] = sharedState->getNextRangeToRead();
    if (startOffset >= endOffset) {
        return false;
    }
    // Output of simple aggregate is guaranteed to be a single value for each aggregate.
    assert(startOffset == 0 && endOffset == 1);
    for (auto i = 0u; i < aggregateVectors.size(); i++) {
        writeAggregateResultToVector(
            *aggregateVectors[i], 0 /* position to write */, sharedState->getAggregateState(i));
    }
    assert(!aggregatesPos.empty());
    outDataChunk->state->initOriginalAndSelectedSize(1);
    metrics->numOutputTuple.increase(outDataChunk->state->selVector->selectedSize);
    return true;
}

} // namespace processor
} // namespace kuzu
