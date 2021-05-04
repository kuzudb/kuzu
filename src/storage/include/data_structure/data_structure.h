#pragma once

#include "src/common/include/configs.h"
#include "src/common/include/vector/value_vector.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/data_structure/data_structure_handle.h"
#include "src/storage/include/data_structure/lists/list_sync_state.h"
#include "src/storage/include/data_structure/utils.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

// DataStructure is the parent class of BaseColumn and BaseLists. It abstracts the state and
// functions that are common in both column and lists, like, 1) layout info (size of a unit of
// element and number of elements that can be accomodated in a page), 2) getting pageIdx and
// pageOffset of an element and, 3) reading from pages.
class DataStructure {

public:
    void reclaim(const unique_ptr<DataStructureHandle>& handle);

    DataType getDataType() { return dataType; }

protected:
    DataStructure(const string& fname, const DataType& dataType, const size_t& elementSize,
        BufferManager& bufferManager);

    virtual ~DataStructure() = default;

    inline PageCursor getPageCursorForOffset(const uint64_t& offset) {
        return PageCursor{
            offset / numElementsPerPage, (uint16_t)((offset % numElementsPerPage) * elementSize)};
    }

    void readBySettingFrame(const shared_ptr<ValueVector>& valueVector,
        const unique_ptr<DataStructureHandle>& handle, PageCursor& pageCursor);

    void readBySequentialCopy(const shared_ptr<ValueVector>& valueVector,
        const unique_ptr<DataStructureHandle>& handle, uint64_t sizeLeftToCopy,
        PageCursor& pageCursor, unique_ptr<LogicalToPhysicalPageIdxMapper> mapper);

protected:
    shared_ptr<spdlog::logger> logger;
    DataType dataType;
    size_t elementSize;
    uint32_t numElementsPerPage;
    FileHandle fileHandle;
    BufferManager& bufferManager;
};

} // namespace storage
} // namespace graphflow
