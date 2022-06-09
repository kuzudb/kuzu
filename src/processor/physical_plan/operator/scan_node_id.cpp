#include "src/processor/include/physical_plan/operator/scan_node_id.h"

namespace graphflow {
namespace processor {

pair<uint64_t, uint64_t> ScanNodeIDSharedState::getNextRangeToRead() {
    auto lck = acquireLock();
    if (currentNodeOffset > maxNodeOffset) {
        return make_pair(currentNodeOffset, currentNodeOffset);
    }
    auto startOffset = currentNodeOffset;
    auto range = min(DEFAULT_VECTOR_CAPACITY, maxNodeOffset + 1 - currentNodeOffset);
    currentNodeOffset += range;
    return make_pair(startOffset, startOffset + range);
}

shared_ptr<ResultSet> ScanNodeID::init(ExecutionContext* context) {
    PhysicalOperator::init(context);
    resultSet = populateResultSet();
    outDataChunk = resultSet->dataChunks[outDataPos.dataChunkPos];
    outValueVector = make_shared<ValueVector>(context->memoryManager, NODE_ID);
    outValueVector->setSequential();
    outDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
    sharedState->initMaxNodeOffset(transaction);
    return resultSet;
}

bool ScanNodeID::getNextTuples() {
    metrics->executionTime.start();
    while (true) {
        auto [startOffset, endOffset] = sharedState->getNextRangeToRead();
        if (startOffset >= endOffset) {
            metrics->executionTime.stop();
            return false;
        }
        auto nodeIDValues = (nodeID_t*)(outValueVector->values);
        auto size = endOffset - startOffset;
        for (auto i = 0u; i < size; ++i) {
            nodeIDValues[i].offset = startOffset + i;
            nodeIDValues[i].label = nodeTable->nodeMetadata->getLabelID();
        }
        outDataChunk->state->initOriginalAndSelectedSize(size);
        outDataChunk->state->resetSelectorToUnselected();
        nodeTable->nodeMetadata->setDeletedNodeOffsetsForMorsel(transaction, outValueVector);
        if (outDataChunk->state->selectedSize <= 0) {
            continue;
        }
        metrics->executionTime.stop();
        metrics->numOutputTuple.increase(outValueVector->state->selectedSize);
        return true;
    }
}

} // namespace processor
} // namespace graphflow
