#include "src/storage/storage_structure/include/lists/list_sync_state.h"

namespace graphflow {
namespace storage {

bool ListSyncState::hasNewRangeToRead() {
    if (!hasValidRangeToRead()) {
        return false;
    }
    if (startIdx + numValuesToRead == numValuesInList) {
        reset();
        return false;
    }
    return true;
}

void ListSyncState::reset() {
    boundNodeOffset = UINT64_MAX;
    startIdx = UINT32_MAX;
    numValuesToRead = UINT32_MAX;
    numValuesInList = UINT64_MAX;
}

} // namespace storage
} // namespace graphflow
