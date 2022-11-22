#include "processor/operator/scan_node_id.h"

namespace kuzu {
namespace processor {

void ScanNodeIDSemiMask::setMask(uint64_t nodeOffset, uint8_t maskerIdx) {
    nodeMask->setMask(nodeOffset, maskerIdx, maskerIdx + 1);
    morselMask->setMask(nodeOffset >> DEFAULT_VECTOR_CAPACITY_LOG_2, maskerIdx, maskerIdx + 1);
}

void ScanNodeIDSharedState::initialize(kuzu::transaction::Transaction* transaction) {
    unique_lock uLck{mtx};
    if (initialized) {
        return;
    }
    maxNodeOffset = nodesStatisticsAndDeletedIDs->getMaxNodeOffset(transaction, tableID);
    maxMorselIdx = maxNodeOffset >> DEFAULT_VECTOR_CAPACITY_LOG_2;
    initialized = true;
}

void ScanNodeIDSharedState::initSemiMask(Transaction* transaction) {
    unique_lock xLck{mtx};
    if (semiMask == nullptr) {
        auto maxNodeOffset_ = nodesStatisticsAndDeletedIDs->getMaxNodeOffset(transaction, tableID);
        semiMask = make_unique<ScanNodeIDSemiMask>(maxNodeOffset_, numMaskers);
    }
}

pair<uint64_t, uint64_t> ScanNodeIDSharedState::getNextRangeToRead() {
    unique_lock lck{mtx};
    // Note: we use maxNodeOffset=UINT64_MAX to represent an empty table.
    if (currentNodeOffset > maxNodeOffset || maxNodeOffset == UINT64_MAX) {
        return make_pair(currentNodeOffset, currentNodeOffset);
    }
    if (semiMask) {
        auto currentMorselIdx = currentNodeOffset >> DEFAULT_VECTOR_CAPACITY_LOG_2;
        assert(currentNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
        while (currentMorselIdx <= maxMorselIdx && !semiMask->isMorselMasked(currentMorselIdx)) {
            currentMorselIdx++;
        }
        currentNodeOffset = min(currentMorselIdx * DEFAULT_VECTOR_CAPACITY, maxNodeOffset);
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
        auto nodeIDValues = (nodeID_t*)(outValueVector->getData());
        auto size = endOffset - startOffset;
        for (auto i = 0u; i < size; ++i) {
            nodeIDValues[i].offset = startOffset + i;
            nodeIDValues[i].tableID = nodeTable->getTableID();
        }
        outDataChunk->state->initOriginalAndSelectedSize(size);
        setSelVector(startOffset, endOffset);
    } while (outDataChunk->state->selVector->selectedSize == 0);
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(outValueVector->state->selVector->selectedSize);
    return true;
}

void ScanNodeID::setSelVector(node_offset_t startOffset, node_offset_t endOffset) {
    if (sharedState->isNodeMaskEnabled()) {
        outDataChunk->state->selVector->resetSelectorToValuePosBuffer();
        // Fill selected positions based on node mask for nodes between the given startOffset and
        // endOffset. If the node is masked (i.e., valid for read), then it is set to the selected
        // positions. Finally, we update the selectedSize for selVector.
        sel_t numSelectedValues = 0;
        for (auto i = 0u; i < (endOffset - startOffset); i++) {
            outDataChunk->state->selVector->selectedPositions[numSelectedValues] = i;
            numSelectedValues += sharedState->getSemiMask()->isNodeMasked(i + startOffset);
        }
        outDataChunk->state->selVector->selectedSize = numSelectedValues;
    } else {
        // By default, the selected positions is set to the const incremental pos array.
        outDataChunk->state->selVector->resetSelectorToUnselected();
    }
    // Apply changes to the selVector from nodes metadata.
    nodeTable->getNodeStatisticsAndDeletedIDs()->setDeletedNodeOffsetsForMorsel(
        transaction, outValueVector, nodeTable->getTableID());
}

} // namespace processor
} // namespace kuzu
