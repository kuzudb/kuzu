#include "src/storage/include/data_structure/data_structure.h"

#include "src/common/include/utils.h"

namespace graphflow {
namespace storage {

DataStructure::DataStructure(const string& fName, const DataType& dataType,
    const size_t& elementSize, BufferManager& bufferManager)
    : dataType{dataType}, elementSize{elementSize}, numElementsPerPage{(uint32_t)(
                                                        PAGE_SIZE / elementSize)},
      logger{LoggerUtils::getOrCreateSpdLogger("storage")}, fileHandle{fName, O_RDWR},
      bufferManager(bufferManager){};

void DataStructure::reclaim(const unique_ptr<DataStructureHandle>& handle) {
    if (handle->hasPageIdx()) {
        bufferManager.unpin(fileHandle, handle->getPageIdx());
        handle->resetPageIdx();
    }
}

void DataStructure::readBySettingFrame(const shared_ptr<ValueVector>& valueVector,
    const unique_ptr<DataStructureHandle>& handle, PageCursor& pageCursor,
    BufferManagerMetrics& metrics) {
    const uint8_t* frame;
    if (handle->getPageIdx() != pageCursor.idx) {
        reclaim(handle);
        handle->setPageIdx(pageCursor.idx);
        frame = bufferManager.pin(fileHandle, pageCursor.idx, metrics);
    } else {
        frame = bufferManager.get(fileHandle, pageCursor.idx, metrics);
    }
    valueVector->values = ((uint8_t*)frame + pageCursor.offset);
}

void DataStructure::readBySequentialCopy(const shared_ptr<ValueVector>& valueVector,
    const unique_ptr<DataStructureHandle>& handle, uint64_t sizeLeftToCopy, PageCursor& pageCursor,
    unique_ptr<LogicalToPhysicalPageIdxMapper> mapper, BufferManagerMetrics& metrics) {
    reclaim(handle);
    valueVector->reset();
    auto values = valueVector->values;
    while (sizeLeftToCopy) {
        uint64_t physicalPageIdx =
            nullptr == mapper ? pageCursor.idx : mapper->getPageIdx(pageCursor.idx);
        auto sizeToCopyInPage = min(PAGE_SIZE - pageCursor.offset, sizeLeftToCopy);
        auto frame = bufferManager.pin(fileHandle, physicalPageIdx, metrics);
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
