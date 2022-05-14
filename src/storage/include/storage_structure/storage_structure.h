#pragma once

#include <utility>

#include "src/common/include/configs.h"
#include "src/common/include/vector/value_vector.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/compression_scheme.h"
#include "src/storage/include/storage_utils.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

// StorageStructure is the parent class of BaseColumn and BaseLists. It abstracts the state and
// functions that are common in both column and lists, like, 1) layout info (size of a unit of
// element and number of elements that can be accommodated in a page), 2) getting pageIdx and
// pageOffset of an element and, 3) reading from pages.
class StorageStructure {

public:
    DataTypeID getDataTypeId() const { return dataType.typeID; }

    // Maps the position of element in page to its byte offset in page.
    inline uint16_t mapElementPosToByteOffset(uint16_t pageElementPos) const {
        return pageElementPos * elementSize;
    }

    inline FileHandle* getFileHandle() { return &fileHandle; }

    inline bool isInMemory() { return isInMemory_; }

protected:
    StorageStructure(const string& fName, const DataType& dataType, const size_t& elementSize,
        BufferManager& bufferManager, bool hasNULLBytes, bool isInMemory);

    virtual ~StorageStructure() {
        if (isInMemory_) {
            StorageStructureUtils::unpinEachPageOfFile(fileHandle, bufferManager);
        }
    }
    void readBySequentialCopy(const shared_ptr<ValueVector>& valueVector, uint64_t sizeLeftToCopy,
        PageElementCursor& cursor,
        const std::function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper);

    void readNodeIDsFromSequentialPages(const shared_ptr<ValueVector>& valueVector,
        PageElementCursor& cursor,
        const std::function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper,
        NodeIDCompressionScheme compressionScheme, bool isAdjLists);

    void readNodeIDsFromAPage(const shared_ptr<ValueVector>& valueVector, uint32_t posInVector,
        uint32_t physicalPageId, uint32_t elementPos, uint64_t numValuesToCopy,
        NodeIDCompressionScheme& compressionScheme, bool isAdjLists);

    static void setNULLBitsForRange(const shared_ptr<ValueVector>& valueVector, uint8_t* frame,
        uint64_t elementPos, uint64_t offsetInVector, uint64_t num);

    static void setNULLBitsForAPos(const shared_ptr<ValueVector>& valueVector, uint8_t* frame,
        uint64_t elementPos, uint64_t offsetInVector);

    static void setNullBitOfAPosInFrame(uint8_t* frame, uint16_t elementPos, bool isNull);

    static inline bool isNullFromNULLByte(uint8_t NULLByte, uint8_t byteLevelPos) {
        return (NULLByte & bitMasksWithSingle1s[byteLevelPos]) > 0;
    }

private:
    static void setNULLBitsFromANULLByte(const shared_ptr<ValueVector>& valueVector,
        pair<uint8_t, uint8_t> NULLByteAndByteLevelStartOffset, uint8_t num,
        uint64_t offsetInVector);

public:
    DataType dataType;
    size_t elementSize;
    uint32_t numElementsPerPage;

protected:
    shared_ptr<spdlog::logger> logger;

    FileHandle fileHandle;
    BufferManager& bufferManager;
    bool isInMemory_;
};

} // namespace storage
} // namespace graphflow
