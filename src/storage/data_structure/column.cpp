#include "src/storage/include/data_structure/column.h"

namespace graphflow {
namespace storage {

void BaseColumn::readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
    const shared_ptr<ValueVector>& valueVector, const unique_ptr<PageHandle>& pageHandle,
    BufferManagerMetrics& metrics) {
    if (nodeIDVector->isSequence()) {
        auto nodeOffset = nodeIDVector->readNodeOffset(0);
        auto pageCursor = getPageCursorForOffset(nodeOffset);
        auto sizeLeftToCopy = nodeIDVector->state->size * elementSize;
        if (pageCursor.offset + sizeLeftToCopy <= PAGE_SIZE) {
            // Case when all the values are in a single page on disk.
            readBySettingFrame(valueVector, *pageHandle, pageCursor, metrics);
        } else {
            // Case when the values are consecutive but not in a single page on disk.
            readBySequentialCopy(
                valueVector, *pageHandle, sizeLeftToCopy, pageCursor,
                [](uint32_t i) {
                    return i;
                } /*no logical-physical page mapping is required for columns*/,
                metrics);
        }
        return;
    }
    // Case when values are at non-sequential locations in a column.
    readFromNonSequentialLocations(nodeIDVector, valueVector, pageHandle, metrics);
}

Literal BaseColumn::readValue(node_offset_t offset, BufferManagerMetrics& metrics) {
    auto cursor = getPageCursorForOffset(offset);
    Literal retVal;
    copyFromAPage((uint8_t*)&retVal.val, cursor.idx, elementSize, cursor.offset, metrics);
    return retVal;
}

void BaseColumn::readFromNonSequentialLocations(const shared_ptr<NodeIDVector>& nodeIDVector,
    const shared_ptr<ValueVector>& valueVector, const unique_ptr<PageHandle>& pageHandle,
    BufferManagerMetrics& metrics) {
    reclaim(*pageHandle);
    valueVector->reset();
    auto values = valueVector->values;
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->getPositionOfCurrIdx();
        auto nodeOffset = nodeIDVector->readNodeOffset(pos);
        auto pageCursor = getPageCursorForOffset(nodeOffset);
        auto frame = bufferManager.pin(fileHandle, pageCursor.idx, metrics);
        memcpy(values + pos * elementSize, frame + pageCursor.offset, elementSize);
        bufferManager.unpin(fileHandle, pageCursor.idx);
    } else {
        for (auto i = 0ul; i < nodeIDVector->state->size; i++) {
            auto nodeOffset =
                nodeIDVector->readNodeOffset(nodeIDVector->state->selectedPositions[i]);
            auto pageCursor = getPageCursorForOffset(nodeOffset);
            auto frame = bufferManager.pin(fileHandle, pageCursor.idx, metrics);
            memcpy(values + valueVector->state->selectedPositions[i] * elementSize,
                frame + pageCursor.offset, elementSize);
            bufferManager.unpin(fileHandle, pageCursor.idx);
        }
    }
}

void Column<STRING>::readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
    const shared_ptr<ValueVector>& valueVector, const unique_ptr<PageHandle>& pageHandle,
    BufferManagerMetrics& metrics) {
    if (nodeIDVector->isSequence()) {
        auto nodeOffset = nodeIDVector->readNodeOffset(0);
        auto pageCursor = getPageCursorForOffset(nodeOffset);
        auto sizeLeftToCopy = nodeIDVector->state->size * elementSize;
        readBySequentialCopy(
            valueVector, *pageHandle, sizeLeftToCopy, pageCursor,
            [](uint32_t i) {
                return i;
            } /*no logical-physical page mapping is required for columns*/,
            metrics);
    } else {
        readFromNonSequentialLocations(nodeIDVector, valueVector, pageHandle, metrics);
    }
    stringOverflowPages.readStringsToVector(*valueVector, metrics);
}

Literal Column<STRING>::readValue(node_offset_t offset, BufferManagerMetrics& metrics) {
    auto cursor = getPageCursorForOffset(offset);
    gf_string_t gfString;
    copyFromAPage((uint8_t*)&gfString, cursor.idx, sizeof(gf_string_t), cursor.offset, metrics);
    Literal retVal;
    retVal.strVal = stringOverflowPages.readString(gfString, metrics);
    return retVal;
}

} // namespace storage
} // namespace graphflow
