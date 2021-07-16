#pragma once

#include "src/common/include/configs.h"
#include "src/common/include/vector/value_vector.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/data_structure/page_handle.h"

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
    void reclaim(PageHandle& pageHandle);

    DataType getDataType() { return dataType; }

    FileHandle* getFileHandle() { return &fileHandle; }

    inline PageCursor getPageCursorForOffset(const uint64_t& offset) {
        return PageCursor{
            offset / numElementsPerPage, (uint16_t)((offset % numElementsPerPage) * elementSize)};
    }

protected:
    DataStructure(const string& fName, const DataType& dataType, const size_t& elementSize,
        BufferManager& bufferManager);

    virtual ~DataStructure() = default;

    void readBySettingFrame(const shared_ptr<ValueVector>& valueVector, PageHandle& pageHandle,
        PageCursor& pageCursor, BufferManagerMetrics& metrics);

    void readBySequentialCopy(const shared_ptr<ValueVector>& valueVector, PageHandle& pageHandle,
        uint64_t sizeLeftToCopy, PageCursor& pageCursor,
        function<uint32_t(uint32_t)> logicalToPhysicalPageMapper, BufferManagerMetrics& metrics);

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
