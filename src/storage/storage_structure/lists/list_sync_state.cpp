#include "src/storage/include/storage_structure/lists/list_sync_state.h"

namespace graphflow {
namespace storage {

bool ListSyncState::hasNewRangeToRead() {
    if (!hasValidRangeToRead()) {
        return false;
    }
    if (startIdx + size == numElements) {
        reset();
        return false;
    }
    return true;
}

void ListSyncState::reset() {
    startIdx = -1u;
    size = -1u;
    numElements = -1u;
}

} // namespace storage
} // namespace graphflow
