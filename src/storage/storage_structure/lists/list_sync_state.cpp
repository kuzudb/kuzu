#include "src/storage/storage_structure/include/lists/list_sync_state.h"

namespace graphflow {
namespace storage {

bool ListSyncState::hasMoreToRead() {
    if (hasValidRangeToRead() && (startElemOffset + numValuesToRead != numValuesInList)) {
        return true;
    }
    if (dataToReadFromUpdateStore && sourceStore == ListSourceStore::PersistentStore) {
        return true;
    }
    return false;
}

void ListSyncState::reset() {
    boundNodeOffset = UINT64_MAX;
    startElemOffset = UINT32_MAX;
    numValuesToRead = UINT32_MAX;
    numValuesInList = UINT64_MAX;
    sourceStore = ListSourceStore::PersistentStore;
    dataToReadFromUpdateStore = false;
}

} // namespace storage
} // namespace graphflow
