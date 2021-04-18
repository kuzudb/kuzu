#include "src/storage/include/structures/column.h"

namespace graphflow {
namespace storage {

void BaseColumn::readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
    const shared_ptr<ValueVector>& valueVector, const unique_ptr<ColumnOrListsHandle>& handle) {
    if (nodeIDVector->isSequence) {
        nodeID_t nodeID;
        nodeIDVector->readNodeOffset(0, nodeID);
        auto pageCursor = getPageCursorForOffset(nodeID.offset);
        auto sizeLeftToCopy = nodeIDVector->state->size * elementSize;
        if (pageCursor.offset + sizeLeftToCopy <= PAGE_SIZE) {
            // Case when all the values are in a single page on disk.
            readBySettingFrame(valueVector, handle, pageCursor);
        } else {
            // Case when the values are consecutive but not in a single page on disk.
            readBySequentialCopy(valueVector, handle, sizeLeftToCopy, pageCursor,
                nullptr /*no page mapping is required*/);
        }
        return;
    }
    // Case when values are at non-sequential locations in a column.
    readFromNonSequentialLocations(nodeIDVector, valueVector, handle);
}

void BaseColumn::readFromNonSequentialLocations(const shared_ptr<NodeIDVector>& nodeIDVector,
    const shared_ptr<ValueVector>& valueVector, const unique_ptr<ColumnOrListsHandle>& handle) {
    reclaim(handle);
    valueVector->reset();
    nodeID_t nodeID;
    auto values = valueVector->values;
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->getCurrSelectedValuesPos();
        nodeIDVector->readNodeOffset(pos, nodeID);
        auto pageCursor = getPageCursorForOffset(nodeID.offset);
        auto frame = bufferManager.pin(fileHandle, pageCursor.idx);
        memcpy(values + pos * elementSize, frame + pageCursor.offset, elementSize);
        bufferManager.unpin(fileHandle, pageCursor.idx);
    } else {
        for (auto i = 0ul; i < nodeIDVector->state->numSelectedValues; i++) {
            nodeIDVector->readNodeOffset(nodeIDVector->state->selectedValuesPos[i], nodeID);
            auto pageCursor = getPageCursorForOffset(nodeID.offset);
            auto frame = bufferManager.pin(fileHandle, pageCursor.idx);
            memcpy(values + valueVector->state->selectedValuesPos[i] * elementSize,
                frame + pageCursor.offset, elementSize);
            bufferManager.unpin(fileHandle, pageCursor.idx);
        }
    }
}

void Column<STRING>::readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
    const shared_ptr<ValueVector>& valueVector, const unique_ptr<ColumnOrListsHandle>& handle) {
    if (nodeIDVector->isSequence) {
        nodeID_t nodeID;
        nodeIDVector->readNodeOffset(0, nodeID);
        auto pageCursor = getPageCursorForOffset(nodeID.offset);
        auto sizeLeftToCopy = nodeIDVector->state->size * elementSize;
        readBySequentialCopy(valueVector, handle, sizeLeftToCopy, pageCursor,
            nullptr /*no page mapping is required*/);
    } else {
        readFromNonSequentialLocations(nodeIDVector, valueVector, handle);
    }
    readStringsFromOverflowPages(valueVector, nodeIDVector->state->size);
}

// TODO: check selectors in the overflow pages as well.
void Column<STRING>::readStringsFromOverflowPages(
    const shared_ptr<ValueVector>& valueVector, uint64_t size) {
    auto values = valueVector->values;
    uint64_t overflowPtr = size * sizeof(gf_string_t);
    size_t sizeNeededForOverflowStrings = size * sizeof(gf_string_t);
    for (auto i = 0u; i < size; i++) {
        if (((gf_string_t*)values)[i].len > 4) {
            sizeNeededForOverflowStrings += ((gf_string_t*)values)[i].len;
        }
    }
    values = valueVector->reserve(sizeNeededForOverflowStrings);
    PageCursor cursor;
    for (auto i = 0u; i < size; i++) {
        if (((gf_string_t*)values)[i].len > 12) {
            ((gf_string_t*)values)[i].getOverflowPtrInfo(cursor.idx, cursor.offset);
            auto frame = bufferManager.pin(overflowPagesFileHandle, cursor.idx);
            memcpy(values + overflowPtr, frame + cursor.offset, ((gf_string_t*)values)[i].len);
            ((gf_string_t*)values)[i].overflowPtr =
                reinterpret_cast<uintptr_t>(values + overflowPtr);
            overflowPtr += ((gf_string_t*)values)[i].len;
            bufferManager.unpin(overflowPagesFileHandle, cursor.idx);
        }
    }
}

} // namespace storage
} // namespace graphflow
