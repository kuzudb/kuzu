#pragma once

#include <memory>

#include "src/storage/include/storage_structure/lists/list_sync_state.h"

namespace graphflow {
namespace storage {

// LargeListHandle stores the state of reading from a large list cross multiple calls to the
// readValues() function. It holds two pieces of information:
// 1. listSyncer: to synchronize reading of values between multiple lists that have related data so
// that they end up reading the correct portion of the list.
// 2. isAdjListsHandle: true, if the largeListHandle stores the state of an AdjLists, else false.
class LargeListHandle {

public:
    LargeListHandle(bool _isAdjListsHandle) : isAdjListsHandle{_isAdjListsHandle} {};

    inline void setListSyncState(shared_ptr<ListSyncState> listSyncState) {
        this->listSyncState = listSyncState;
    }
    inline shared_ptr<ListSyncState> getListSyncState() { return listSyncState; }

    bool hasMoreToRead() {
        return isAdjListsHandle ? listSyncState->hasNewRangeToRead() :
                                  listSyncState->hasValidRangeToRead();
    }

    void reset() { listSyncState->reset(); }

private:
    shared_ptr<ListSyncState> listSyncState;
    bool isAdjListsHandle;
};

} // namespace storage
} // namespace graphflow
