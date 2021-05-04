#include "src/storage/include/data_structure/data_structure.h"

namespace graphflow {
namespace storage {

DataStructure::DataStructure(const string& fname, const DataType& dataType,
    const size_t& elementSize, BufferManager& bufferManager)
    : logger{spdlog::get("storage")}, dataType{dataType}, elementSize{elementSize},
      numElementsPerPage{(uint32_t)(PAGE_SIZE / elementSize)}, fileHandle{fname},
      bufferManager(bufferManager){};

void DataStructure::reclaim(const unique_ptr<DataStructureHandle>& handle) {
    if (handle->hasPageIdx()) {
        bufferManager.unpin(fileHandle, handle->getPageIdx());
        handle->resetPageIdx();
    }
}

void DataStructure::readBySettingFrame(const shared_ptr<ValueVector>& valueVector,
    const unique_ptr<DataStructureHandle>& handle, PageCursor& pageCursor) {
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

void DataStructure::readBySequentialCopy(const shared_ptr<ValueVector>& valueVector,
    const unique_ptr<DataStructureHandle>& handle, uint64_t sizeLeftToCopy, PageCursor& pageCursor,
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
