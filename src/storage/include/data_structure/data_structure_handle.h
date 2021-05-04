#pragma once

#include "src/common/include/configs.h"
#include "src/common/include/vector/value_vector.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/data_structure/lists/list_sync_state.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

// DataStructureHandle stores the state of reading from a list or a column across multiple calls to
// the readValues() function. It holds three pieces information:
// 1. pageIdx: reference of the page that is currently pinned by the Column/Lists to make the Frame
// available to the readValues() caller.
// 2. listSyncer: to synchronize reading of values between multiple lists that have related data so
// that they end up reading the correct portion of the list.
// 3. isAdjListsHandle: true, if the handle stores the state of anAdjLists, else false.
class DataStructureHandle {

public:
    DataStructureHandle() : pageIdx{-1u}, isAdjListsHandle{false} {};

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

    bool hasMoreToRead() {
        return isAdjListsHandle ? listSyncState->hasNewRangeToRead() :
                                  listSyncState->hasValidRangeToRead();
    }

private:
    uint32_t pageIdx;
    shared_ptr<ListSyncState> listSyncState;
    bool isAdjListsHandle;
};

} // namespace storage
} // namespace graphflow
