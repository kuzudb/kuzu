#include "src/storage/include/data_structure/column.h"

namespace graphflow {
namespace storage {

void Column::readValues(const shared_ptr<ValueVector>& nodeIDVector,
    const shared_ptr<ValueVector>& valueVector, BufferManagerMetrics& metrics) {
    assert(nodeIDVector->dataType == NODE);
    if (nodeIDVector->isSequence) {
        auto nodeOffset = ((nodeID_t*)nodeIDVector->values)[0].offset;
        auto pageCursor = getPageCursorForOffset(nodeOffset);
        auto sizeLeftToCopy = nodeIDVector->state->originalSize * elementSize;
        readBySequentialCopy(
            valueVector, sizeLeftToCopy, pageCursor,
            [](uint32_t i) {
                return i;
            } /*no logical-physical page mapping is required for columns*/,
            metrics);
        return;
    }
    // Case when values are at non-sequential locations in a column.
    readFromNonSequentialLocations(nodeIDVector, valueVector, metrics);
}

Literal Column::readValue(node_offset_t offset, BufferManagerMetrics& metrics) {
    auto cursor = getPageCursorForOffset(offset);
    Literal retVal;
    copyFromAPage((uint8_t*)&retVal.val, cursor.idx, elementSize, cursor.offset, metrics);
    return retVal;
}

void Column::readFromNonSequentialLocations(const shared_ptr<ValueVector>& nodeIDVector,
    const shared_ptr<ValueVector>& valueVector, BufferManagerMetrics& metrics) {
    auto values = valueVector->values;
    auto nodeIDValues = (nodeID_t*)nodeIDVector->values;
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->getPositionOfCurrIdx();
        auto nodeOffset = nodeIDValues[pos].offset;
        auto pageCursor = getPageCursorForOffset(nodeOffset);
        auto frame = bufferManager.pin(fileHandle, pageCursor.idx, metrics);
        memcpy(values + pos * elementSize, frame + pageCursor.offset, elementSize);
        bufferManager.unpin(fileHandle, pageCursor.idx);
    } else {
        for (auto i = 0ul; i < nodeIDVector->state->selectedSize; i++) {
            auto nodeOffset = nodeIDValues[nodeIDVector->state->selectedPositions[i]].offset;
            auto pageCursor = getPageCursorForOffset(nodeOffset);
            auto frame = bufferManager.pin(fileHandle, pageCursor.idx, metrics);
            memcpy(values + valueVector->state->selectedPositions[i] * elementSize,
                frame + pageCursor.offset, elementSize);
            bufferManager.unpin(fileHandle, pageCursor.idx);
        }
    }
}

void StringPropertyColumn::readValues(const shared_ptr<ValueVector>& nodeIDVector,
    const shared_ptr<ValueVector>& valueVector, BufferManagerMetrics& metrics) {
    assert(nodeIDVector->dataType == NODE);
    auto nodeIDValues = (nodeID_t*)nodeIDVector->values;
    if (nodeIDVector->isSequence) {
        auto nodeOffset = nodeIDValues[0].offset;
        auto pageCursor = getPageCursorForOffset(nodeOffset);
        auto sizeLeftToCopy = nodeIDVector->state->selectedSize * elementSize;
        readBySequentialCopy(
            valueVector, sizeLeftToCopy, pageCursor,
            [](uint32_t i) {
                return i;
            } /*no logical-physical page mapping is required for columns*/,
            metrics);
    } else {
        readFromNonSequentialLocations(nodeIDVector, valueVector, metrics);
    }
    stringOverflowPages.readStringsToVector(*valueVector, metrics);
}

Literal StringPropertyColumn::readValue(node_offset_t offset, BufferManagerMetrics& metrics) {
    auto cursor = getPageCursorForOffset(offset);
    gf_string_t gfString;
    copyFromAPage((uint8_t*)&gfString, cursor.idx, sizeof(gf_string_t), cursor.offset, metrics);
    Literal retVal;
    retVal.strVal = stringOverflowPages.readString(gfString, metrics);
    return retVal;
}

void AdjColumn::readValues(const shared_ptr<ValueVector>& nodeIDVector,
    const shared_ptr<ValueVector>& resultVector, BufferManagerMetrics& metrics) {
    assert(nodeIDVector->dataType == NODE && nodeIDVector->state == resultVector->state);
    auto nodeIDValues = (nodeID_t*)nodeIDVector->values;
    auto resultVectorValues = (nodeID_t*)resultVector->values;
    if (nodeIDVector->isSequence) {
        auto pageCursor = getPageCursorForOffset(nodeIDValues[0].offset);
        readNodeIDsFromSequentialPages(
            resultVector, pageCursor, [](uint64_t i) { return i; }, nodeIDCompressionScheme,
            metrics);
    } else {
        auto numValuesToCopy =
            nodeIDVector->state->isFlat() ? 1 : nodeIDVector->state->selectedSize;
        for (auto i = 0u; i < numValuesToCopy; i++) {
            auto pos = nodeIDVector->state->isFlat() ? nodeIDVector->state->getPositionOfCurrIdx() :
                                                       nodeIDVector->state->selectedPositions[i];
            auto nodeOffset = nodeIDValues[pos].offset;
            auto pageCursor = getPageCursorForOffset(nodeOffset);
            readNodeIDsFromAPage(&resultVectorValues[i], pageCursor.idx, pageCursor.offset,
                1 /* numValuesToCopy */, nodeIDCompressionScheme, metrics);
        }
    }
}

} // namespace storage
} // namespace graphflow
