#pragma once

#include <cstdint>

#include "src/common/types/include/node_id_t.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

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
        this->startIdx = startIdx_;
        this->numValuesToRead = numValuesToRead_;
    }
    inline node_offset_t getBoundNodeOffset() const { return boundNodeOffset; };
    inline uint32_t getStartIdx() const { return startIdx; }
    inline uint32_t getEndIdx() const { return startIdx + numValuesToRead; }
    inline bool hasValidRangeToRead() const { return UINT32_MAX != startIdx; }

    bool hasNewRangeToRead();
    void reset();

private:
    node_offset_t boundNodeOffset;
    uint32_t startIdx;
    uint32_t numValuesToRead;
    uint64_t numValuesInList;
};

} // namespace storage
} // namespace graphflow
