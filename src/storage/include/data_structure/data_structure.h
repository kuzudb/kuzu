#pragma once

#include "src/common/include/compression_scheme.h"
#include "src/common/include/configs.h"
#include "src/common/include/vector/value_vector.h"
#include "src/storage/include/buffer_manager.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

// Holds the reference to a location in a collection of pages of a Column or Lists.
struct PageCursor {

    PageCursor(uint64_t idx, uint16_t offset) : idx{idx}, offset{offset} {};
    PageCursor() : PageCursor{-1ul, (uint16_t)-1} {};

    uint64_t idx;
    uint16_t offset;
};

// DataStructure is the parent class of BaseColumn and BaseLists. It abstracts the state and
// functions that are common in both column and lists, like, 1) layout info (size of a unit of
// element and number of elements that can be accommodated in a page), 2) getting pageIdx and
// pageOffset of an element and, 3) reading from pages.
class DataStructure {

public:
    DataType getDataType() const { return dataType; }

    FileHandle* getFileHandle() { return &fileHandle; }

    inline PageCursor getPageCursorForOffset(const uint64_t& elementOffset) const {
        return PageCursor{elementOffset / numElementsPerPage,
            (uint16_t)((elementOffset % numElementsPerPage) * elementSize)};
    }

protected:
    DataStructure(const string& fName, const DataType& dataType, const size_t& elementSize,
        BufferManager& bufferManager, bool isInMemory);

    virtual ~DataStructure() = default;

    void readBySequentialCopy(const shared_ptr<ValueVector>& valueVector, uint64_t sizeLeftToCopy,
        PageCursor& pageCursor,
        const std::function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper,
        BufferManagerMetrics& metrics);

    void readNodeIDsFromSequentialPages(const shared_ptr<ValueVector>& valueVector,
        PageCursor& pageCursor,
        const std::function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper,
        NodeIDCompressionScheme compressionScheme, BufferManagerMetrics& metrics);

    void copyFromAPage(uint8_t* values, uint32_t physicalPageIdx, uint64_t sizeToCopy,
        uint32_t pageOffset, BufferManagerMetrics& metrics);

    void readNodeIDsFromAPage(const shared_ptr<ValueVector>& valueVector, uint32_t posInVector,
        uint32_t physicalPageId, uint32_t pageOffset, uint64_t numValuesToCopy,
        NodeIDCompressionScheme& compressionScheme, BufferManagerMetrics& metrics);

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
