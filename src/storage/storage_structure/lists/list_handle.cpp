#include "storage/storage_structure/lists/list_handle.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

bool ListSyncState::hasMoreAndSwitchSourceIfNecessary() {
    if (hasValidRangeToRead() && hasMoreLeftInList()) {
        // Has more in the current source store.
        return true;
    }
    if (sourceStore == ListSourceStore::PERSISTENT_STORE && numValuesInUpdateStore > 0) {
        // Switch from PERSISTENT_STORE to UPDATE_STORE.
        switchToUpdateStore();
        return true;
    }
    return false;
}

void ListSyncState::resetState() {
    boundNodeOffset = INVALID_OFFSET;
    startElemOffset = UINT32_MAX;
    numValuesToRead = UINT32_MAX;
    numValuesInUpdateStore = 0;
    numValuesInPersistentStore = 0;
    sourceStore = ListSourceStore::PERSISTENT_STORE;
}

} // namespace storage
} // namespace kuzu
