#pragma once

#include "src/common/include/compression_scheme.h"
#include "src/common/include/configs.h"
#include "src/common/include/vector/value_vector.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/data_structure/page_utils.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

// DataStructure is the parent class of BaseColumn and BaseLists. It abstracts the state and
// functions that are common in both column and lists, like, 1) layout info (size of a unit of
// element and number of elements that can be accommodated in a page), 2) getting pageIdx and
// pageOffset of an element and, 3) reading from pages.
class DataStructure {

public:
    DataType getDataType() const { return dataType; }

    // Maps the position of element in page to its byte offset in page.
    inline uint16_t mapElementPosToByteOffset(uint16_t pageElementPos) const {
        return pageElementPos * elementSize;
    }

protected:
    DataStructure(const string& fName, const DataType& dataType, const size_t& elementSize,
        BufferManager& bufferManager, bool hasNULLBytes, bool isInMemory);

    virtual ~DataStructure() = default;

    void readBySequentialCopy(const shared_ptr<ValueVector>& valueVector, uint64_t sizeLeftToCopy,
        PageElementCursor& cursor,
        const std::function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper);

    void readNodeIDsFromSequentialPages(const shared_ptr<ValueVector>& valueVector,
        PageElementCursor& cursor,
        const std::function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper,
        NodeIDCompressionScheme compressionScheme, bool isAdjLists);

    void readNodeIDsFromAPage(const shared_ptr<ValueVector>& valueVector, uint32_t posInVector,
        uint32_t physicalPageId, uint32_t posInPage, uint64_t numValuesToCopy,
        NodeIDCompressionScheme& compressionScheme, bool isAdjLists);

    static void setNULLBitsForRange(const shared_ptr<ValueVector>& valueVector,
        const uint8_t* frame, uint64_t elementPos, uint64_t offsetInVector, uint64_t num);

    static void setNULLBitsForAPos(const shared_ptr<ValueVector>& valueVector, const uint8_t* frame,
        uint64_t elementPos, uint64_t offsetInVector);

private:
    static void setNULLBitsFromANULLByte(const shared_ptr<ValueVector>& valueVector,
        uint8_t NULLByte, uint8_t num, uint64_t startPos, uint64_t offsetInVector);

public:
    DataType dataType;
    size_t elementSize;
    uint32_t numElementsPerPage;

protected:
    shared_ptr<spdlog::logger> logger;

    FileHandle fileHandle;
    BufferManager& bufferManager;
};

} // namespace storage
} // namespace graphflow
