#include "src/storage/include/structures/common.h"

namespace graphflow {
namespace storage {

bool ListSyncState::hasNewRangeToRead() {
    if (!hasValidRangeToRead()) {
        return false;
    }
    if (startIdx + size == numElements) {
        reset();
        return false;
    }
    return true;
}

void ListSyncState::reset() {
    startIdx = -1u;
    size = -1u;
    numElements = -1u;
}

bool ColumnOrListsHandle::hasMoreToRead() {
    if (isAdjListsHandle) {
        return listSyncState->hasNewRangeToRead();
    } else {
        return listSyncState->hasValidRangeToRead();
    }
}

BaseColumnOrLists::BaseColumnOrLists(const string& fname, const DataType& dataType,
    const size_t& elementSize, BufferManager& bufferManager)
    : logger{spdlog::get("storage")}, dataType{dataType}, elementSize{elementSize},
      numElementsPerPage{(uint32_t)(PAGE_SIZE / elementSize)}, fileHandle{fname},
      bufferManager(bufferManager){};

void BaseColumnOrLists::reclaim(const unique_ptr<ColumnOrListsHandle>& handle) {
    if (handle->hasPageIdx()) {
        bufferManager.unpin(fileHandle, handle->getPageIdx());
        handle->resetPageIdx();
    }
}

void BaseColumnOrLists::readBySettingFrame(const shared_ptr<ValueVector>& valueVector,
    const unique_ptr<ColumnOrListsHandle>& handle, PageCursor& pageCursor) {
    const uint8_t* frame;
    if (handle->getPageIdx() != pageCursor.idx) {
        reclaim(handle);
        handle->setPageIdx(pageCursor.idx);
        frame = bufferManager.pin(fileHandle, pageCursor.idx);
    } else {
        frame = bufferManager.get(fileHandle, pageCursor.idx);
    }
    valueVector->values = ((uint8_t*)frame + pageCursor.offset);
}

void BaseColumnOrLists::readBySequentialCopy(const shared_ptr<ValueVector>& valueVector,
    const unique_ptr<ColumnOrListsHandle>& handle, uint64_t sizeLeftToCopy, PageCursor& pageCursor,
    unique_ptr<LogicalToPhysicalPageIdxMapper> mapper) {
    reclaim(handle);
    valueVector->reset();
    auto values = valueVector->values;
    while (sizeLeftToCopy) {
        uint64_t physicalPageIdx =
            nullptr == mapper ? pageCursor.idx : mapper->getPageIdx(pageCursor.idx);
        auto sizeToCopyInPage = min(PAGE_SIZE - pageCursor.offset, sizeLeftToCopy);
        auto frame = bufferManager.pin(fileHandle, physicalPageIdx);
        memcpy(values, frame + pageCursor.offset, sizeToCopyInPage);
        bufferManager.unpin(fileHandle, physicalPageIdx);
        values += sizeToCopyInPage;
        sizeLeftToCopy -= sizeToCopyInPage;
        pageCursor.offset = 0;
        pageCursor.idx++;
    }
}

} // namespace storage
} // namespace graphflow
