#include "src/storage/include/data_structure/data_structure.h"

#include <iostream>

#include "src/common/include/utils.h"

namespace graphflow {
namespace storage {

DataStructure::DataStructure(const string& fName, const DataType& dataType,
    const size_t& elementSize, BufferManager& bufferManager, bool isInMemory)
    : dataType{dataType}, elementSize{elementSize},
      numElementsPerPage((uint32_t)(PAGE_SIZE / elementSize)),
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

void DataStructure::readNodeIDsFromSequentialPages(const shared_ptr<ValueVector>& valueVector,
    PageCursor& pageCursor, const std::function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper,
    NodeIDCompressionScheme compressionScheme, BufferManagerMetrics& metrics) {
    auto posInVector = 0;
    auto numValuesToCopy = valueVector->state->originalSize;
    while (numValuesToCopy > 0) {
        auto numValuesToCopyInPage =
            min(numValuesToCopy, (uint64_t)numElementsPerPage - pageCursor.offset / elementSize);
        auto physicalPageId = logicalToPhysicalPageMapper(pageCursor.idx);
        readNodeIDsFromAPage(valueVector, posInVector, physicalPageId, pageCursor.offset,
            numValuesToCopyInPage, compressionScheme, metrics);
        posInVector += numValuesToCopyInPage;
        pageCursor.idx++;
        pageCursor.offset = 0;
        numValuesToCopy -= numValuesToCopyInPage;
    }
}

void DataStructure::copyFromAPage(uint8_t* values, uint32_t physicalPageIdx, uint64_t sizeToCopy,
    uint32_t pageOffset, BufferManagerMetrics& metrics) {
    auto frame = bufferManager.pin(fileHandle, physicalPageIdx, metrics);
    memcpy(values, frame + pageOffset, sizeToCopy);
    bufferManager.unpin(fileHandle, physicalPageIdx);
}

void DataStructure::readNodeIDsFromAPage(const shared_ptr<ValueVector>& valueVector,
    uint32_t posInVector, uint32_t physicalPageId, uint32_t pageOffset, uint64_t numValuesToCopy,
    NodeIDCompressionScheme& compressionScheme, BufferManagerMetrics& metrics) {
    auto nullOffset = compressionScheme.getNodeOffsetNullValue();
    auto labelSize = compressionScheme.getNumBytesForLabel();
    auto offsetSize = compressionScheme.getNumBytesForOffset();
    auto values = (nodeID_t*)valueVector->values;
    auto frame = bufferManager.pin(fileHandle, physicalPageId, metrics);
    frame += pageOffset;
    for (auto i = 0u; i < numValuesToCopy; i++) {
        nodeID_t nodeID{0, 0};
        memcpy(&nodeID.label, frame, labelSize);
        memcpy(&nodeID.offset, frame + labelSize, offsetSize);
        // Set common label if label size is 0
        nodeID.label = labelSize == 0 ? compressionScheme.getCommonLabel() : nodeID.label;
        valueVector->setNull(posInVector, nodeID.offset == nullOffset);
        values[posInVector++] = nodeID;
        frame += (labelSize + offsetSize);
    }
    bufferManager.unpin(fileHandle, physicalPageId);
}

} // namespace storage
} // namespace graphflow
