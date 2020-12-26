#include "src/storage/include/structures/column.h"

namespace graphflow {
namespace storage {

void BaseColumn::readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
    const shared_ptr<ValueVector>& valueVector, const uint64_t& size,
    const unique_ptr<VectorFrameHandle>& handle) {
    if (nodeIDVector->getIsSequence()) {
        nodeID_t nodeID;
        nodeIDVector->readValue(0, nodeID);
        auto startOffset = nodeID.offset;
        auto pageOffset = startOffset % numElementsPerPage;
        if (pageOffset + size <= numElementsPerPage) {
            readFromSeqNodeIDsBySettingFrame(valueVector, handle, startOffset);
        } else {
            readFromSeqNodeIDsByCopying(valueVector, size, handle, startOffset);
        }
        return;
    }
    readFromNonSeqNodeIDs(nodeIDVector, valueVector, size, handle);
}

void BaseColumn::reclaim(unique_ptr<VectorFrameHandle>& handle) {
    if (handle->isFrameBound) {
        bufferManager.unpin(fileHandle, handle->pageIdx);
    }
}

void BaseColumn::readFromSeqNodeIDsBySettingFrame(const shared_ptr<ValueVector>& valueVector,
    const unique_ptr<VectorFrameHandle>& handle, node_offset_t startOffset) {
    handle->pageIdx = getPageIdx(startOffset);
    handle->isFrameBound = true;
    auto frame = bufferManager.pin(fileHandle, handle->pageIdx);
    valueVector->setValues((uint8_t*)frame);
}

void BaseColumn::readFromSeqNodeIDsByCopying(const shared_ptr<ValueVector>& valueVector,
    const uint64_t& size, const unique_ptr<VectorFrameHandle>& handle, node_offset_t startOffset) {
    handle->isFrameBound = false;
    valueVector->reset();
    auto values = valueVector->getValues();
    auto numBytesLeftToCopy = size * elementSize;
    auto pageOffset = getPageOffset(startOffset);
    auto pageIdx = getPageIdx(startOffset);
    while (numBytesLeftToCopy) {
        auto numBytesToCopyInPage = min(numBytesLeftToCopy, PAGE_SIZE) - pageOffset;
        auto frame = bufferManager.pin(fileHandle, pageIdx);
        memcpy(values, frame + pageOffset, numBytesToCopyInPage);
        bufferManager.unpin(fileHandle, pageIdx);
        values += numBytesToCopyInPage;
        numBytesLeftToCopy -= numBytesToCopyInPage;
        pageOffset = 0;
        pageIdx++;
    }
}

void BaseColumn::readFromNonSeqNodeIDs(const shared_ptr<NodeIDVector>& nodeIDVector,
    const shared_ptr<ValueVector>& valueVector, const uint64_t& size,
    const unique_ptr<VectorFrameHandle>& handle) {
    handle->isFrameBound = false;
    valueVector->reset();
    nodeID_t nodeID;
    auto values = valueVector->getValues();
    for (uint64_t i = 0; i < size; i++) {
        nodeIDVector->readValue(i, nodeID);
        auto pageIdx = getPageIdx(nodeID.offset);
        auto frame = bufferManager.pin(fileHandle, pageIdx);
        memcpy(values + (i * elementSize), frame + getPageOffset(nodeID.offset), elementSize);
        bufferManager.unpin(fileHandle, pageIdx);
    }
}

void Column<gf_string_t>::readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
    const shared_ptr<ValueVector>& valueVector, const uint64_t& size,
    const unique_ptr<VectorFrameHandle>& handle) {
    nodeID_t nodeID;
    nodeIDVector->readValue(0, nodeID);
    auto startOffset = nodeID.offset;
    if (nodeIDVector->getIsSequence()) {
        readFromSeqNodeIDsByCopying(valueVector, size, handle, startOffset);
    } else {
        readFromNonSeqNodeIDs(nodeIDVector, valueVector, size, handle);
    }
    readStringsFromOverflowPages(valueVector, size);
}

void Column<gf_string_t>::readStringsFromOverflowPages(
    const shared_ptr<ValueVector>& valueVector, const uint64_t& size) {
    auto values = valueVector->getValues();
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
        if (((gf_string_t*)values)[i].len > 4) {
            ((gf_string_t*)values)[i].setOverflowPtrToPageCursor(cursor);
            auto frame = bufferManager.pin(overflowPagesFileHandle, cursor.idx);
            memcpy(values + overflowPtr, frame + cursor.offset, ((gf_string_t*)values)[i].len);
            ((gf_string_t*)values)[i].overflowPtr = overflowPtr;
            overflowPtr += ((gf_string_t*)values)[i].len;
            bufferManager.unpin(overflowPagesFileHandle, cursor.idx);
        }
    }
}

} // namespace storage
} // namespace graphflow
