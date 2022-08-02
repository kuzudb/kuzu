#include "src/processor/include/physical_plan/operator/scan_node_id.h"

namespace graphflow {
namespace processor {

pair<uint64_t, uint64_t> ScanNodeIDSharedState::getNextRangeToRead() {
    unique_lock lck{mtx};
    // Note: we use maxNodeOffset=UINT64_MAX to represent an empty table.
    if (currentNodeOffset > maxNodeOffset || maxNodeOffset == UINT64_MAX) {
        return make_pair(currentNodeOffset, currentNodeOffset);
    }
    if (hasSemiMask) {
        auto maxNumMorsels =
            (maxNodeOffset + DEFAULT_VECTOR_CAPACITY - 1) / DEFAULT_VECTOR_CAPACITY;
        auto currentMorselIdx = currentNodeOffset / DEFAULT_VECTOR_CAPACITY;
        assert(currentNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
        while (!morselMask[currentMorselIdx]) {
            if (currentMorselIdx >= (maxNumMorsels - 1)) {
                break;
            }
            currentMorselIdx++;
        }
        currentNodeOffset = currentMorselIdx * DEFAULT_VECTOR_CAPACITY;
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

void ScanNodeID::setSelectedPositions(
    node_offset_t startOffset, node_offset_t endOffset, SelectionVector& selVector) {
    selVector.resetSelectorToValuePosBuffer();
    sel_t numSelectedValues = 0;
    for (auto i = 0u; i < (endOffset - startOffset); i++) {
        selVector.selectedPositions[numSelectedValues] = i;
        numSelectedValues += sharedState->getMaskForNode(i + startOffset);
    }
    selVector.selectedSize = numSelectedValues;
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
            nodeIDValues[i].label = nodeTable->labelID;
        }
        outDataChunk->state->initOriginalAndSelectedSize(size);
        outDataChunk->state->selVector->resetSelectorToUnselected();
        if (sharedState->getHasSemiMask()) {
            setSelectedPositions(startOffset, endOffset, *outDataChunk->state->selVector);
        }
        nodeTable->getNodesMetadata()->setDeletedNodeOffsetsForMorsel(
            transaction, outValueVector, nodeTable->getLabelID());
        if (outDataChunk->state->selVector->selectedSize <= 0) {
            continue;
        }
        metrics->executionTime.stop();
        metrics->numOutputTuple.increase(outValueVector->state->selVector->selectedSize);
        return true;
    }
}

} // namespace processor
} // namespace graphflow
