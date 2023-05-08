#pragma once

#include "common/types/types.h"
#include "storage/storage_structure/lists/list_headers.h"
#include "storage/storage_structure/lists/lists_metadata.h"

namespace kuzu {
namespace storage {

enum class ListSourceStore : uint8_t {
    PERSISTENT_STORE = 0,
    UPDATE_STORE = 1,
};

struct ListHandle;
// ListSyncState holds the data that is required to synchronize reading from multiple Lists that
// have related data and hence share same AdjListHeaders. The Lists that share a single copy of
// AdjListHeaders are - edges, i.e., adjLists of a rel table in a particular direction, e.g.,
// forward or backward, and the properties of those edges. For the case of reading from a large
// list, we do not / cannot read the entire list in a single operation since it can be very big,
// hence we read in batches from a definite start point to an end point. List Sync holds this
// information and helps in co-ordinating all related lists so that all of them read the correct
// portion of data.
class ListSyncState {

    friend class ListHandle;

public:
    explicit ListSyncState() { resetState(); };

    inline bool isBoundNodeOffsetInValid() const {
        return boundNodeOffset == common::INVALID_OFFSET;
    }

    bool hasMoreAndSwitchSourceIfNecessary();

    void resetState();

private:
    inline bool hasValidRangeToRead() const { return UINT32_MAX != startElemOffset; }
    inline csr_offset_t getNumValuesInList() {
        return sourceStore == ListSourceStore::PERSISTENT_STORE ? numValuesInPersistentStore :
                                                                  numValuesInUpdateStore;
    }
    inline bool hasMoreLeftInList() {
        return (startElemOffset + numValuesToRead) < getNumValuesInList();
    }
    inline void switchToUpdateStore() {
        sourceStore = ListSourceStore::UPDATE_STORE;
        startElemOffset = UINT32_MAX;
    }

private:
    common::offset_t boundNodeOffset;
    uint32_t numValuesInUpdateStore;
    uint32_t numValuesInPersistentStore;
    uint32_t startElemOffset;
    uint32_t numValuesToRead;
    ListSourceStore sourceStore;
};

struct ListHandle {
    explicit ListHandle(ListSyncState& listSyncState) : listSyncState{listSyncState} {}

    static inline std::function<uint32_t(uint32_t)> getPageMapper(
        ListsMetadata& listMetadata, common::offset_t nodeOffset) {
        return listMetadata.getPageMapperForChunkIdx(StorageUtils::getListChunkIdx(nodeOffset));
    }
    inline void setMapper(ListsMetadata& listMetadata) {
        mapper = getPageMapper(listMetadata, listSyncState.boundNodeOffset);
    }
    inline void resetSyncState() { listSyncState.resetState(); }
    inline void initSyncState(common::offset_t boundNodeOffset, uint64_t numValuesInUpdateStore,
        uint64_t numValuesInPersistentStore, ListSourceStore sourceStore) {
        listSyncState.boundNodeOffset = boundNodeOffset;
        listSyncState.numValuesInUpdateStore = numValuesInUpdateStore;
        listSyncState.numValuesInPersistentStore = numValuesInPersistentStore;
        listSyncState.sourceStore = sourceStore;
    }
    inline common::offset_t getBoundNodeOffset() const { return listSyncState.boundNodeOffset; }
    inline ListSourceStore getListSourceStore() { return listSyncState.sourceStore; }
    inline uint32_t getStartElemOffset() const { return listSyncState.startElemOffset; }
    inline uint32_t getEndElemOffset() const {
        return listSyncState.startElemOffset + listSyncState.numValuesToRead;
    }
    inline uint32_t getNumValuesToRead() const { return listSyncState.numValuesToRead; }
    inline uint32_t getNumValuesInList() {
        return listSyncState.sourceStore == ListSourceStore::PERSISTENT_STORE ?
                   listSyncState.numValuesInPersistentStore :
                   listSyncState.numValuesInUpdateStore;
    }
    inline bool hasValidRangeToRead() { return listSyncState.hasValidRangeToRead(); }
    inline void setRangeToRead(uint32_t startIdx, uint32_t numValuesToRead) {
        listSyncState.startElemOffset = startIdx;
        listSyncState.numValuesToRead = numValuesToRead;
    }
    inline bool hasMoreAndSwitchSourceIfNecessary() {
        return listSyncState.hasMoreAndSwitchSourceIfNecessary();
    }

private:
    ListSyncState& listSyncState;

public:
    std::function<uint32_t(uint32_t)> mapper;
};

} // namespace storage
} // namespace kuzu
