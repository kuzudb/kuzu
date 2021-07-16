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

void DataStructure::reclaim(PageHandle& pageHandle) {
    if (pageHandle.hasPageIdx()) {
        bufferManager.unpin(fileHandle, pageHandle.getPageIdx());
        pageHandle.resetPageIdx();
    }
}

void DataStructure::readBySettingFrame(const shared_ptr<ValueVector>& valueVector,
    PageHandle& pageHandle, PageCursor& pageCursor, BufferManagerMetrics& metrics) {
    const uint8_t* frame;
    if (pageHandle.getPageIdx() != pageCursor.idx) {
        reclaim(pageHandle);
        pageHandle.setPageIdx(pageCursor.idx);
        frame = bufferManager.pin(fileHandle, pageCursor.idx, metrics);
    } else {
        frame = bufferManager.get(fileHandle, pageCursor.idx, metrics);
    }
    valueVector->values = ((uint8_t*)frame + pageCursor.offset);
}

void DataStructure::readBySequentialCopy(const shared_ptr<ValueVector>& valueVector,
    PageHandle& pageHandle, uint64_t sizeLeftToCopy, PageCursor& pageCursor,
    function<uint32_t(uint32_t)> logicalToPhysicalPageMapper, BufferManagerMetrics& metrics) {
    reclaim(pageHandle);
    valueVector->reset();
    auto values = valueVector->values;
    while (sizeLeftToCopy) {
        auto physicalPageIdx = logicalToPhysicalPageMapper(pageCursor.idx);
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
