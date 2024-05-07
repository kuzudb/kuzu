#include "processor/operator/scan_node_id.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace processor {

std::pair<offset_t, offset_t> NodeTableScanState::getNextRangeToRead() {
    // Note: we use maxNodeOffset=UINT64_MAX to represent an empty table.
    if (currentNodeOffset > maxNodeOffset || maxNodeOffset == INVALID_OFFSET) {
        return std::make_pair(currentNodeOffset, currentNodeOffset);
    }
    if (isSemiMaskEnabled()) {
        auto currentMorselIdx = MaskUtil::getMorselIdx(currentNodeOffset);
        KU_ASSERT(currentNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
        while (currentMorselIdx <= maxMorselIdx && !semiMask->isMorselMasked(currentMorselIdx)) {
            currentMorselIdx++;
        }
        currentNodeOffset = std::min(currentMorselIdx * DEFAULT_VECTOR_CAPACITY, maxNodeOffset);
    }
    auto startOffset = currentNodeOffset;
    auto range = std::min(DEFAULT_VECTOR_CAPACITY, maxNodeOffset + 1 - currentNodeOffset);
    currentNodeOffset += range;
    return std::make_pair(startOffset, startOffset + range);
}

void ScanNodeIDSharedState::initialize(Transaction* transaction) {
    auto numMask = tableStates[0]->getSemiMask()->getNumMasks();
    numNodes = 0;
    for (auto& tableState : tableStates) {
        KU_ASSERT(tableState->getSemiMask()->getNumMasks() == numMask);
        tableState->initializeMaxOffset(transaction);
        numNodes += tableState->getTable()->getNumTuples(transaction);
    }
    (void)numMask; // For clang-tidy: used for assert.
    numNodesScanned = 0;
}

std::tuple<NodeTableScanState*, offset_t, offset_t> ScanNodeIDSharedState::getNextRangeToRead() {
    std::unique_lock lck{mtx};
    if (currentStateIdx == tableStates.size()) {
        return std::make_tuple(nullptr, INVALID_OFFSET, INVALID_OFFSET);
    }
    auto [startOffset, endOffset] = tableStates[currentStateIdx]->getNextRangeToRead();
    while (startOffset >= endOffset) {
        currentStateIdx++;
        if (currentStateIdx == tableStates.size()) {
            return std::make_tuple(nullptr, INVALID_OFFSET, INVALID_OFFSET);
        }
        auto [_startOffset, _endOffset] = tableStates[currentStateIdx]->getNextRangeToRead();
        startOffset = _startOffset;
        endOffset = _endOffset;
    }
    KU_ASSERT(currentStateIdx < tableStates.size());
    numNodesScanned += endOffset - startOffset;
    return std::make_tuple(tableStates[currentStateIdx].get(), startOffset, endOffset);
}

void ScanNodeID::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* /*context*/) {
    outValueVector = resultSet->getValueVector(outDataPos);
    outValueVector->setSequential();
}

bool ScanNodeID::getNextTuplesInternal(ExecutionContext* context) {
    do {
        auto [state, startOffset, endOffset] = sharedState->getNextRangeToRead();
        if (state == nullptr) {
            return false;
        }
        outValueVector->state->getSelVectorUnsafe().setToUnfiltered();
        auto nodeIDValues = (nodeID_t*)(outValueVector->getData());
        auto size = endOffset - startOffset;
        for (auto i = 0u; i < size; ++i) {
            nodeIDValues[i].offset = startOffset + i;
            nodeIDValues[i].tableID = state->getTable()->getTableID();
        }
        outValueVector->state->initOriginalAndSelectedSize(size);
        setSelVector(context, state, startOffset, endOffset);
    } while (outValueVector->state->getSelVector().getSelSize() == 0);
    metrics->numOutputTuple.increase(outValueVector->state->getSelVector().getSelSize());
    return true;
}

void ScanNodeID::setSelVector(ExecutionContext* context, NodeTableScanState* tableState,
    offset_t startOffset, offset_t endOffset) {
    // Apply changes to the selVector from nodes metadata.
    tableState->getTable()->setSelVectorForDeletedOffsets(context->clientContext->getTx(),
        outValueVector.get());
    if (tableState->isSemiMaskEnabled()) {
        auto buffer = outValueVector->state->getSelVectorUnsafe().getMultableBuffer();
        sel_t prevSelectedSize = outValueVector->state->getSelVector().getSelSize();
        // Fill selected positions based on node mask for nodes between the given startOffset and
        // endOffset. If the node is masked (i.e., valid for read), then it is set to the selected
        // positions. Finally, we update the selectedSize for selVector.
        sel_t numSelectedValues = 0;
        for (auto i = 0u; i < (endOffset - startOffset); i++) {
            buffer[numSelectedValues] = i;
            numSelectedValues += tableState->getSemiMask()->isNodeMasked(i + startOffset);
        }
        outValueVector->state->getSelVectorUnsafe().setSelSize(numSelectedValues);
        if (prevSelectedSize != numSelectedValues) {
            outValueVector->state->getSelVectorUnsafe().setToFiltered();
        }
    }
}

double ScanNodeID::getProgress(ExecutionContext* /*context*/) const {
    uint64_t numNodes = sharedState->getNumNodes();
    uint64_t numReadNodes = sharedState->getNumNodesScanned();
    return numNodes == 0 ? 0.0 : (double)numReadNodes / numNodes;
}

} // namespace processor
} // namespace kuzu
