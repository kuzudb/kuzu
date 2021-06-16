#include "src/storage/include/data_structure/column.h"

namespace graphflow {
namespace storage {

void BaseColumn::readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
    const shared_ptr<ValueVector>& valueVector, const unique_ptr<DataStructureHandle>& handle,
    BufferManagerMetrics& metrics) {
    if (nodeIDVector->isSequence()) {
        auto nodeOffset = nodeIDVector->readNodeOffset(0);
        auto pageCursor = getPageCursorForOffset(nodeOffset);
        auto sizeLeftToCopy = nodeIDVector->state->size * elementSize;
        if (pageCursor.offset + sizeLeftToCopy <= PAGE_SIZE) {
            // Case when all the values are in a single page on disk.
            readBySettingFrame(valueVector, handle, pageCursor, metrics);
        } else {
            // Case when the values are consecutive but not in a single page on disk.
            readBySequentialCopy(valueVector, handle, sizeLeftToCopy, pageCursor,
                nullptr /*no page mapping is required*/, metrics);
        }
        return;
    }
    // Case when values are at non-sequential locations in a column.
    readFromNonSequentialLocations(nodeIDVector, valueVector, handle, metrics);
}

void BaseColumn::readFromNonSequentialLocations(const shared_ptr<NodeIDVector>& nodeIDVector,
    const shared_ptr<ValueVector>& valueVector, const unique_ptr<DataStructureHandle>& handle,
    BufferManagerMetrics& metrics) {
    reclaim(handle);
    valueVector->reset();
    auto values = valueVector->values;
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->getCurrSelectedValuesPos();
        auto nodeOffset = nodeIDVector->readNodeOffset(pos);
        auto pageCursor = getPageCursorForOffset(nodeOffset);
        auto frame = bufferManager.pin(fileHandle, pageCursor.idx, metrics);
        memcpy(values + pos * elementSize, frame + pageCursor.offset, elementSize);
        bufferManager.unpin(fileHandle, pageCursor.idx);
    } else {
        for (auto i = 0ul; i < nodeIDVector->state->size; i++) {
            auto nodeOffset =
                nodeIDVector->readNodeOffset(nodeIDVector->state->selectedValuesPos[i]);
            auto pageCursor = getPageCursorForOffset(nodeOffset);
            auto frame = bufferManager.pin(fileHandle, pageCursor.idx, metrics);
            memcpy(values + valueVector->state->selectedValuesPos[i] * elementSize,
                frame + pageCursor.offset, elementSize);
            bufferManager.unpin(fileHandle, pageCursor.idx);
        }
    }
}

void Column<STRING>::readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
    const shared_ptr<ValueVector>& valueVector, const unique_ptr<DataStructureHandle>& handle,
    BufferManagerMetrics& metrics) {
    if (nodeIDVector->isSequence()) {
        auto nodeOffset = nodeIDVector->readNodeOffset(0);
        auto pageCursor = getPageCursorForOffset(nodeOffset);
        auto sizeLeftToCopy = nodeIDVector->state->size * elementSize;
        readBySequentialCopy(valueVector, handle, sizeLeftToCopy, pageCursor,
            nullptr /*no page mapping is required*/, metrics);
    } else {
        readFromNonSequentialLocations(nodeIDVector, valueVector, handle, metrics);
    }
    readStringsFromOverflowPages(valueVector, metrics);
}

void Column<STRING>::readStringsFromOverflowPages(
    const shared_ptr<ValueVector>& valueVector, BufferManagerMetrics& metrics) {
    PageCursor cursor;
    for (auto i = 0u; i < valueVector->state->size; i++) {
        auto pos = valueVector->state->selectedValuesPos[i];
        auto& value = ((gf_string_t*)valueVector->values)[pos];
        if (value.len > gf_string_t::SHORT_STR_LENGTH) {
            value.getOverflowPtrInfo(cursor.idx, cursor.offset);
            auto frame = bufferManager.pin(overflowPagesFileHandle, cursor.idx, metrics);
            auto copyStr = new char[value.len];
            memcpy(copyStr, frame + cursor.offset, value.len);
            value.overflowPtr = reinterpret_cast<uintptr_t>(copyStr);
            bufferManager.unpin(overflowPagesFileHandle, cursor.idx);
        }
    }
}

} // namespace storage
} // namespace graphflow
