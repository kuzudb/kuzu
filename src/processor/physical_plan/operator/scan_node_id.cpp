#include "src/processor/include/physical_plan/operator/scan_node_id.h"

namespace graphflow {
namespace processor {

void ScanNodeIDSharedState::initialize(graphflow::transaction::Transaction* transaction) {
    unique_lock uLck{mtx};
    if (initialized) {
        return;
    }
    maxNodeOffset = nodesMetadata->getMaxNodeOffset(transaction, labelID);
    maxMorselIdx = maxNodeOffset >> DEFAULT_VECTOR_CAPACITY_LOG_2;
    initialized = true;
}

pair<uint64_t, uint64_t> ScanNodeIDSharedState::getNextRangeToRead() {
    unique_lock lck{mtx};
    // Note: we use maxNodeOffset=UINT64_MAX to represent an empty table.
    if (currentNodeOffset > maxNodeOffset || maxNodeOffset == UINT64_MAX) {
        return make_pair(currentNodeOffset, currentNodeOffset);
    }
    if (mask) {
        auto currentMorselIdx = currentNodeOffset >> DEFAULT_VECTOR_CAPACITY_LOG_2;
        assert(currentNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
        while (currentMorselIdx <= maxMorselIdx && !mask->morselMask[currentMorselIdx]) {
            currentMorselIdx++;
        }
        currentNodeOffset = min(currentMorselIdx * DEFAULT_VECTOR_CAPACITY, maxNodeOffset);
    }
    auto startOffset = currentNodeOffset;
    auto range = min(DEFAULT_VECTOR_CAPACITY, maxNodeOffset + 1 - currentNodeOffset);
    currentNodeOffset += range;
    return make_pair(startOffset, startOffset + range);
}

void ScanNodeIDSharedState::initSemiMask(graphflow::transaction::Transaction* transaction) {
    unique_lock xLck{mtx};
    if (mask == nullptr) {
        auto maxNodeOffset_ = nodesMetadata->getMaxNodeOffset(transaction, labelID);
        auto maxMorselIdx_ = maxNodeOffset_ >> DEFAULT_VECTOR_CAPACITY_LOG_2;
        mask = make_unique<SemiMask>(maxNodeOffset_, maxMorselIdx_);
    }
}

shared_ptr<ResultSet> ScanNodeID::init(ExecutionContext* context) {
    PhysicalOperator::init(context);
    resultSet = populateResultSet();
    outDataChunk = resultSet->dataChunks[outDataPos.dataChunkPos];
    outValueVector = make_shared<ValueVector>(NODE_ID, context->memoryManager);
    outValueVector->setSequential();
    outDataChunk->insert(outDataPos.valueVectorPos, outValueVector);
    sharedState->initialize(transaction);
    return resultSet;
}

bool ScanNodeID::getNextTuples() {
    metrics->executionTime.start();
    do {
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
        setSelVector(startOffset, endOffset);
    } while (outDataChunk->state->selVector->selectedSize == 0);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(outValueVector->state->selVector->selectedSize);
    return true;
}

void ScanNodeID::setSelVector(node_offset_t startOffset, node_offset_t endOffset) {
    if (sharedState->isSemiMaskEnabled()) {
        outDataChunk->state->selVector->resetSelectorToValuePosBuffer();
        // Fill selected positions based on node mask for nodes between the given startOffset and
        // endOffset. If the node is masked (i.e., valid for read), then it is set to the selected
        // positions. Finally, we update the selectedSize for selVector.
        sel_t numSelectedValues = 0;
        for (auto i = 0u; i < (endOffset - startOffset); i++) {
            outDataChunk->state->selVector->selectedPositions[numSelectedValues] = i;
            // TODO(Guodong): sharedState->isNodeMasked(i + startOffset) doesn't guarantee to be 0/1
            // on my platform.
            numSelectedValues += sharedState->isNodeMasked(i + startOffset);
        }
        outDataChunk->state->selVector->selectedSize = numSelectedValues;
    } else {
        // By default, the selected positions is set to the const incremental pos array.
        outDataChunk->state->selVector->resetSelectorToUnselected();
    }
    // Apply changes to the selVector from nodes metadata.
    nodeTable->getNodesMetadata()->setDeletedNodeOffsetsForMorsel(
        transaction, outValueVector, nodeTable->getLabelID());
}

} // namespace processor
} // namespace graphflow
