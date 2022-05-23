#include "src/storage/include/storage_structure/storage_structure.h"

#include "src/common/include/utils.h"

namespace graphflow {
namespace storage {

StorageStructure::StorageStructure(const string& fName, const DataType& dataType,
    const size_t& elementSize, BufferManager& bufferManager, bool hasNULLBytes, bool isInMemory)
    : dataType{dataType}, elementSize{elementSize}, logger{LoggerUtils::getOrCreateSpdLogger(
                                                        "storage")},
      fileHandle{fName, FileHandle::O_DefaultPagedExistingDBFileDoNotCreate},
      bufferManager{bufferManager}, isInMemory_{isInMemory} {
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
        auto frame =
            bufferManager.pin(fileHandle, physicalPageIdx);
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
    uint32_t posInVector, uint32_t physicalPageId, uint32_t elementPos, uint64_t numValuesToCopy,
    NodeIDCompressionScheme& compressionScheme, bool isAdjLists) {
    auto nodeValues = (nodeID_t*)valueVector->values;
    auto labelSize = compressionScheme.getNumBytesForLabel();
    auto offsetSize = compressionScheme.getNumBytesForOffset();
    auto frame = bufferManager.pin(fileHandle, physicalPageId);
    if (isAdjLists) {
        valueVector->setRangeNonNull(posInVector, numValuesToCopy);
    } else {
        setNULLBitsForRange(valueVector, frame, elementPos, posInVector, numValuesToCopy);
    }
    frame += mapElementPosToByteOffset(elementPos);
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
    uint8_t* frame, uint64_t elementPos, uint64_t offsetInVector, uint64_t num) {
    while (num) {
        auto numInCurrentByte = min(num, 8 - (elementPos % 8));
        auto NULLByteAndByteLevelOffset =
            PageUtils::getNULLByteAndByteLevelOffsetPair(frame, elementPos);
        setNULLBitsFromANULLByte(
            valueVector, NULLByteAndByteLevelOffset, numInCurrentByte, offsetInVector);
        elementPos += numInCurrentByte;
        offsetInVector += numInCurrentByte;
        num -= numInCurrentByte;
    }
}

void StorageStructure::setNULLBitsForAPos(const shared_ptr<ValueVector>& valueVector,
    uint8_t* frame, uint64_t elementPos, uint64_t offsetInVector) {
    auto NULLByteAndByteLevelOffset =
        PageUtils::getNULLByteAndByteLevelOffsetPair(frame, elementPos);
    setNULLBitsFromANULLByte(valueVector, NULLByteAndByteLevelOffset, 1, offsetInVector);
}

void StorageStructure::setNULLBitsFromANULLByte(const shared_ptr<ValueVector>& valueVector,
    pair<uint8_t, uint8_t> NULLByteAndByteLevelStartOffset, uint8_t num, uint64_t offsetInVector) {
    auto NULLByte = NULLByteAndByteLevelStartOffset.first;
    auto startPos = NULLByteAndByteLevelStartOffset.second;
    while (num--) {
        valueVector->setNull(offsetInVector++, isNullFromNULLByte(NULLByte, startPos++));
    }
}

void StorageStructure::setNullBitOfAPosInFrame(uint8_t* frame, uint16_t elementPos, bool isNull) {
    auto NULLBytePtrAndByteLevelOffset =
        PageUtils::getNULLBytePtrAndByteLevelOffsetPair(frame, elementPos);
    uint8_t NULLByte = *NULLBytePtrAndByteLevelOffset.first;
    if (isNull) {
        *NULLBytePtrAndByteLevelOffset.first =
            NULLByte | bitMasksWithSingle1s[NULLBytePtrAndByteLevelOffset.second];
    } else {
        *NULLBytePtrAndByteLevelOffset.first =
            NULLByte & bitMasksWithSingle0s[NULLBytePtrAndByteLevelOffset.second];
    }
}

} // namespace storage
} // namespace graphflow
