#pragma once

#include <cstdint>

#include "common/types/node_id_t.h"
#include "common/types/types.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

enum class ListSourceStore : uint8_t {
    PersistentStore = 0,
    ListsUpdateStore = 1,
};

// ListSyncState holds the data that is required to synchronize reading from multiple Lists that
// have related data and hence share same AdjListHeaders. The Lists that share a single copy of
// AdjListHeaders are - edges, i.e., adjLists of a rel table in a particular direction, e.g.,
// forward or backward, and the properties of those edges. For the case of reading from a large
// list, we do not / cannot read the entire list in a single operation since it can be very big,
// hence we read in batches from a definite start point to an end point. List Sync holds this
// information and helps in co-ordinating all related lists so that all of them read the correct
// portion of data.
class ListSyncState {

public:
    ListSyncState() { reset(); };

    inline void setNumValuesInList(uint64_t numValuesInList_) {
        this->numValuesInList = numValuesInList_;
    }
    inline void setBoundNodeOffset(node_offset_t boundNodeOffset_) {
        this->boundNodeOffset = boundNodeOffset_;
    }
    inline void setRangeToRead(uint32_t startIdx_, uint32_t numValuesToRead_) {
        this->startElemOffset = startIdx_;
        this->numValuesToRead = numValuesToRead_;
    }
    inline node_offset_t getBoundNodeOffset() const { return boundNodeOffset; };
    inline uint32_t getStartElemOffset() const { return startElemOffset; }
    inline uint32_t getEndElemOffset() const { return startElemOffset + numValuesToRead; }
    inline bool hasValidRangeToRead() const { return UINT32_MAX != startElemOffset; }
    inline uint32_t getNumValuesToRead() const { return numValuesToRead; }
    inline uint64_t getNumValuesInList() const { return numValuesInList; }
    inline ListSourceStore getListSourceStore() const { return sourceStore; }
    inline void setSourceStore(ListSourceStore sourceStore) { this->sourceStore = sourceStore; }
    inline void setDataToReadFromUpdateStore(bool dataToReadFromUpdateStore_) {
        dataToReadFromUpdateStore = dataToReadFromUpdateStore_;
    }
    inline bool hasDataToReadFromUpdateStore() const { return dataToReadFromUpdateStore; }
    inline list_header_t getListHeader() const { return listHeader; }
    inline void setListHeader(list_header_t listHeader_) { listHeader = listHeader_; }

    bool hasMoreToRead();
    void reset();

private:
    node_offset_t boundNodeOffset;
    list_header_t listHeader;
    uint32_t startElemOffset;
    uint32_t numValuesToRead;
    uint64_t numValuesInList;
    ListSourceStore sourceStore;
    bool dataToReadFromUpdateStore;
};

} // namespace storage
} // namespace kuzu
