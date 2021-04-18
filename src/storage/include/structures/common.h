#pragma once

#include "src/common/include/configs.h"
#include "src/common/include/vector/value_vector.h"
#include "src/storage/include/buffer_manager.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

// Holds the reference to a location in a collection of pages of a Column or Lists.
typedef struct PageCursor {

    PageCursor(uint64_t idx, uint16_t offset) : idx{idx}, offset{offset} {};
    PageCursor() : PageCursor{-1ul, (uint16_t)-1} {};

    uint64_t idx;
    uint16_t offset;

} PageCursor;

// ListSyncState holds the data that is required to synchronize reading from multiple Lists that
// have related data and hence share same AdjListHeaders. The Lists that share a single copy of
// AdjListHeaders are - a single-directional edges of a rel label and edges' properties. For the
// case of reading from a large list, we do not / cannot read the entire list in a single
// operation since it can be very big, hence we read in batches from a definite start point to an
// end point. List Sync holds this information and helps in co-ordinating all related lists so
// that all of them read the correct portion of data.
class ListSyncState {

public:
    ListSyncState() { reset(); };

    inline void init(uint64_t numElements) { this->numElements = numElements; }

    inline void set(uint32_t startIdx, uint32_t size) {
        this->startIdx = startIdx;
        this->size = size;
    }

    uint32_t getStartIdx() { return startIdx; }
    uint32_t getEndIdx() { return startIdx + size; }
    uint32_t getSize() { return size; }

    bool hasNewRangeToRead();
    inline bool hasValidRangeToRead() { return -1u != startIdx; }

private:
    void reset();

private:
    uint32_t startIdx;
    uint32_t size;
    uint64_t numElements;
};

// ColumnOrListsHandle stores the state of reading a list or from a column across multiple calls to
// the readValues() function. It holds three pieces information:
// 1. pageIdx: reference of the page that is currently pinned by the Column/Lists to make the Frame
// available to the readValues() caller.
// 2. listSyncer: to synchronize reading of values between multiple lists that have related data so
// that they end up reading the correct portion of the list.
// 3. isAdjListsHandle: true, if the handle stores the state of anAdjLists, else false.
class ColumnOrListsHandle {

public:
    ColumnOrListsHandle() : pageIdx{-1u}, isAdjListsHandle{false} {};

    inline void setIsAdjListHandle() { isAdjListsHandle = true; }
    inline bool getIsAdjListsHandle() { return isAdjListsHandle; }

    inline void setListSyncState(shared_ptr<ListSyncState> listSyncState) {
        this->listSyncState = listSyncState;
    }
    inline shared_ptr<ListSyncState> getListSyncState() { return listSyncState; }

    inline bool hasPageIdx() { return -1u != pageIdx; }
    inline uint32_t getPageIdx() { return pageIdx; }
    inline void setPageIdx(uint32_t pageIdx) { this->pageIdx = pageIdx; }
    inline void resetPageIdx() { pageIdx = -1u; }

    bool hasMoreToRead();

private:
    uint32_t pageIdx;
    shared_ptr<ListSyncState> listSyncState;
    bool isAdjListsHandle;
};

// LogicalToPhysicalPageIDxMapper maps a logical pageIdx where a particular piece of data is located
// to the actual sequential index of the page in the file. If the data to be read is a small list,
// the logical pageIdx corresponds to the sequence of the page in a particular chunk while if the
// data is being read from a large list, the logical page idx is one of the pages which stores the
// complete large list. Note that pages of allocated to a chunk or a large list is not contiguously
// arragened in the lists file. For the case of reading from a column, we do not need a mapper since
// the logical page idx corresponds to the sequence in which pages are arranged in a column file.
class LogicalToPhysicalPageIdxMapper {

public:
    LogicalToPhysicalPageIdxMapper(const vector<uint64_t>& map, uint32_t baseOffset)
        : map{map}, baseOffset{baseOffset} {}

    uint64_t getPageIdx(uint64_t pageIdx) { return map[pageIdx + baseOffset]; }

private:
    const vector<uint64_t>& map;
    uint32_t baseOffset;
};

// Common class that is extended by both BaseColumn and BaseLists class. It abstracts the state and
// functions that common in both column and lists, like, 1) layout info (size of a unit of element
// and number of elements that can be accomodated in a page), 2) getting pageIdx and pageOffset of
// an element and, 3) reading from pages.
class BaseColumnOrLists {

public:
    void reclaim(const unique_ptr<ColumnOrListsHandle>& handle);

    DataType getDataType() { return dataType; }

protected:
    BaseColumnOrLists(const string& fname, const DataType& dataType, const size_t& elementSize,
        BufferManager& bufferManager);

    virtual ~BaseColumnOrLists() = default;

    inline PageCursor getPageCursorForOffset(const uint64_t& offset) {
        return PageCursor{
            offset / numElementsPerPage, (uint16_t)((offset % numElementsPerPage) * elementSize)};
    }

    void readBySettingFrame(const shared_ptr<ValueVector>& valueVector,
        const unique_ptr<ColumnOrListsHandle>& handle, PageCursor& pageCursor);

    void readBySequentialCopy(const shared_ptr<ValueVector>& valueVector,
        const unique_ptr<ColumnOrListsHandle>& handle, uint64_t sizeLeftToCopy,
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
