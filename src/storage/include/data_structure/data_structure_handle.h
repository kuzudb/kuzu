#pragma once

#include "src/common/include/configs.h"
#include "src/common/include/vector/value_vector.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/data_structure/lists/list_sync_state.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

// PageHandle stores the state a pageIdx when reading from a list or a column across multiple calls
// to the readValues() function. The pageIdx is the pageIdx of the page that is currently pinned by
// the Column/Lists to make the Frame available to the readValues() caller.
class PageHandle {

public:
    PageHandle() : pageIdx{-1u} {};

    inline bool hasPageIdx() { return -1u != pageIdx; }
    inline uint32_t getPageIdx() { return pageIdx; }
    inline void setPageIdx(uint32_t pageIdx) { this->pageIdx = pageIdx; }
    inline void resetPageIdx() { pageIdx = -1u; }

private:
    uint32_t pageIdx;
};

// ListHandle stores the state of reading from a list cross multiple calls to
// the readValues() function. It extends PageHandle, so stores a pinned pageIdx. In addition it
// holds two other pieces information:
// 1. listSyncer: to synchronize reading of values between multiple lists that have related data so
// that they end up reading the correct portion of the list.
// 2. isAdjListsHandle: true, if the listHandle stores the state of anAdjLists, else false.
class ListHandle : public PageHandle {

public:
    ListHandle() : PageHandle(), isAdjListsHandle{false} {};

    inline void setIsAdjListHandle() { isAdjListsHandle = true; }
    inline bool getIsAdjListsHandle() { return isAdjListsHandle; }

    inline void setListSyncState(shared_ptr<ListSyncState> listSyncState) {
        this->listSyncState = listSyncState;
    }
    inline shared_ptr<ListSyncState> getListSyncState() { return listSyncState; }

    bool hasMoreToRead() {
        return isAdjListsHandle ? listSyncState->hasNewRangeToRead() :
                                  listSyncState->hasValidRangeToRead();
    }

private:
    shared_ptr<ListSyncState> listSyncState;
    bool isAdjListsHandle;
};
} // namespace storage
} // namespace graphflow
