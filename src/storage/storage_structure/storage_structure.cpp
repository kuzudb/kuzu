#include "src/storage/include/storage_structure/storage_structure.h"

#include "src/common/include/utils.h"

namespace graphflow {
namespace storage {

StorageStructure::StorageStructure(const string& fName, const DataType& dataType,
    const size_t& elementSize, BufferManager& bufferManager, bool hasNULLBytes, bool isInMemory)
    : dataType{dataType}, elementSize{elementSize}, logger{LoggerUtils::getOrCreateSpdLogger(
                                                        "storage")},
      fileHandle{fName, FileHandle::O_DefaultPagedExistingDBFile}, bufferManager{bufferManager},
      isInMemory{isInMemory} {
    numElementsPerPage = hasNULLBytes ?
                             PageUtils::getNumElementsInAPageWithNULLBytes(elementSize) :
                             PageUtils::getNumElementsInAPageWithoutNULLBytes(elementSize);
    if (isInMemory) {
        StorageStructureUtils::pinEachPageOfFile(fileHandle, bufferManager);
    }
}

void StorageStructure::readBySequentialCopy(const shared_ptr<ValueVector>& valueVector,
    uint64_t sizeLeftToCopy, PageElementCursor& cursor,
    const function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper) {
    auto values = valueVector->values;
    auto offsetInVector = 0;
    while (sizeLeftToCopy) {
        auto physicalPageIdx = logicalToPhysicalPageMapper(cursor.idx);
        auto sizeToCopyInPage =
            min((uint64_t)(numElementsPerPage - cursor.pos) * elementSize, sizeLeftToCopy);
        auto numValuesToCopyInPage = sizeToCopyInPage / elementSize;
        auto frame = bufferManager.pin(fileHandle, physicalPageIdx);
        memcpy(values, frame + mapElementPosToByteOffset(cursor.pos), sizeToCopyInPage);
        setNULLBitsForRange(valueVector, frame, cursor.pos, offsetInVector, numValuesToCopyInPage);
        bufferManager.unpin(fileHandle, physicalPageIdx);
        values += sizeToCopyInPage;
        sizeLeftToCopy -= sizeToCopyInPage;
        offsetInVector += numValuesToCopyInPage;
        cursor.pos = 0;
        cursor.idx++;
    }
}

void StorageStructure::readNodeIDsFromSequentialPages(const shared_ptr<ValueVector>& valueVector,
    PageElementCursor& cursor, const std::function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper,
    NodeIDCompressionScheme compressionScheme, bool isAdjLists) {
    auto numValuesToCopy = valueVector->state->originalSize;
    auto posInVector = 0;
    while (numValuesToCopy > 0) {
        auto numValuesToCopyInPage =
            min(numValuesToCopy, (uint64_t)(numElementsPerPage - cursor.pos));
        auto physicalPageId = logicalToPhysicalPageMapper(cursor.idx);
        readNodeIDsFromAPage(valueVector, posInVector, physicalPageId, cursor.pos,
            numValuesToCopyInPage, compressionScheme, isAdjLists);
        cursor.idx++;
        cursor.pos = 0;
        numValuesToCopy -= numValuesToCopyInPage;
        posInVector += numValuesToCopyInPage;
    }
}

void StorageStructure::readNodeIDsFromAPage(const shared_ptr<ValueVector>& valueVector,
    uint32_t posInVector, uint32_t physicalPageId, uint32_t posInPage, uint64_t numValuesToCopy,
    NodeIDCompressionScheme& compressionScheme, bool isAdjLists) {
    auto nodeValues = (nodeID_t*)valueVector->values;
    auto labelSize = compressionScheme.getNumBytesForLabel();
    auto offsetSize = compressionScheme.getNumBytesForOffset();
    auto frame = bufferManager.pin(fileHandle, physicalPageId);
    if (isAdjLists) {
        valueVector->setRangeNonNull(posInVector, numValuesToCopy);
    } else {
        setNULLBitsForRange(valueVector, frame, posInPage, posInVector, numValuesToCopy);
    }
    frame += mapElementPosToByteOffset(posInPage);
    for (auto i = 0u; i < numValuesToCopy; i++) {
        nodeID_t nodeID{0, 0};
        memcpy(&nodeID.label, frame, labelSize);
        memcpy(&nodeID.offset, frame + labelSize, offsetSize);
        nodeID.label = labelSize == 0 ? compressionScheme.getCommonLabel() : nodeID.label;
        nodeValues[posInVector + i] = nodeID;
        frame += (labelSize + offsetSize);
    }
    bufferManager.unpin(fileHandle, physicalPageId);
}

void StorageStructure::setNULLBitsForRange(const shared_ptr<ValueVector>& valueVector,
    const uint8_t* frame, uint64_t elementPos, uint64_t offsetInVector, uint64_t num) {
    while (num) {
        auto numInCurrentByte = min(num, 8 - (elementPos % 8));
        auto NULLByte = PageUtils::getNULLByteForOffset(frame, elementPos);
        setNULLBitsFromANULLByte(
            valueVector, NULLByte, numInCurrentByte, elementPos, offsetInVector);
        elementPos += numInCurrentByte;
        offsetInVector += numInCurrentByte;
        num -= numInCurrentByte;
    }
}

void StorageStructure::setNULLBitsForAPos(const shared_ptr<ValueVector>& valueVector,
    const uint8_t* frame, uint64_t elementPos, uint64_t offsetInVector) {
    auto NULLByte = PageUtils::getNULLByteForOffset(frame, elementPos);
    setNULLBitsFromANULLByte(valueVector, NULLByte, 1, elementPos, offsetInVector);
}

void StorageStructure::setNULLBitsFromANULLByte(const shared_ptr<ValueVector>& valueVector,
    uint8_t NULLByte, uint8_t num, uint64_t startPos, uint64_t offsetInVector) {
    while (num--) {
        auto maskedNULLByte = 0;
        switch (startPos++) {
        case 0:
            maskedNULLByte = NULLByte & 0b10000000;
            break;
        case 1:
            maskedNULLByte = NULLByte & 0b01000000;
            break;
        case 2:
            maskedNULLByte = NULLByte & 0b00100000;
            break;
        case 3:
            maskedNULLByte = NULLByte & 0b00010000;
            break;
        case 4:
            maskedNULLByte = NULLByte & 0b00001000;
            break;
        case 5:
            maskedNULLByte = NULLByte & 0b00000100;
            break;
        case 6:
            maskedNULLByte = NULLByte & 0b00000010;
            break;
        case 7:
            maskedNULLByte = NULLByte & 0b00000001;
            break;
        }
        valueVector->setNull(offsetInVector++, maskedNULLByte > 0);
    }
}

} // namespace storage
} // namespace graphflow
