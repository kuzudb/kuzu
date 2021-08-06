#include "src/storage/include/data_structure/data_structure.h"

#include "src/common/include/utils.h"

namespace graphflow {
namespace storage {

DataStructure::DataStructure(const string& fName, const DataType& dataType,
    const size_t& elementSize, BufferManager& bufferManager, bool isInMemory)
    : dataType{dataType}, elementSize{elementSize}, numElementsPerPage{(uint32_t)(
                                                        PAGE_SIZE / elementSize)},
      logger{LoggerUtils::getOrCreateSpdLogger("storage")}, fileHandle{fName, O_RDWR, isInMemory},
      bufferManager(bufferManager){};

void DataStructure::readBySequentialCopy(const shared_ptr<ValueVector>& valueVector,
    uint64_t sizeLeftToCopy, PageCursor& pageCursor,
    const function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper,
    BufferManagerMetrics& metrics) {
    auto values = valueVector->values;
    while (sizeLeftToCopy) {
        auto physicalPageIdx = logicalToPhysicalPageMapper(pageCursor.idx);
        auto sizeToCopyInPage =
            min(((uint64_t)numElementsPerPage * elementSize) - pageCursor.offset, sizeLeftToCopy);
        copyFromAPage(values, physicalPageIdx, sizeToCopyInPage, pageCursor.offset, metrics);
        values += sizeToCopyInPage;
        sizeLeftToCopy -= sizeToCopyInPage;
        pageCursor.offset = 0;
        pageCursor.idx++;
    }
}

void DataStructure::copyFromAPage(uint8_t* values, uint32_t physicalPageIdx, uint64_t sizeToCopy,
    uint32_t pageOffset, BufferManagerMetrics& metrics) {
    auto frame = bufferManager.pin(fileHandle, physicalPageIdx, metrics);
    memcpy(values, frame + pageOffset, sizeToCopy);
    bufferManager.unpin(fileHandle, physicalPageIdx);
}

} // namespace storage
} // namespace graphflow
