#pragma once

#include "src/storage/include/data_structure/lists/list_sync_state.h"

namespace graphflow {
namespace storage {

// ListsPageHandle stores the state of reading from a list cross multiple calls to the readValues()
// function. It extends PageHandle, so stores a pinned pageIdx. In addition it holds two other
// pieces information:
// 1. listSyncer: to synchronize reading of values between multiple lists that have related data so
// that they end up reading the correct portion of the list.
// 2. isAdjListsHandle: true, if the listsPageHandle stores the state of anAdjLists, else false.
class ListsPageHandle : public PageHandle {

public:
    ListsPageHandle() : PageHandle(), isAdjListsHandle{false} {};

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
