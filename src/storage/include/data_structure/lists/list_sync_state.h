#pragma once

#include <cstdint>

namespace graphflow {
namespace storage {

// ListSyncState holds the data that is required to synchronize reading from multiple Lists that
// have related data and hence share same AdjListHeaders. The Lists that share a single copy of
// AdjListHeaders are - edges, i.e., adjlists of a rel label in a particular direction, e.g.,
// forward or backward, and the properties of those edges. For the case of reading from a large
// list, we do not / cannot read the entire list in a single operation since it can be very big,
// hence we read in batches from a definite start point to an end point. List Sync holds this
// information and helps in co-ordinating all related lists so that all of them read the correct
// portion of data.
class ListSyncState {

public:
    ListSyncState() { reset(); };

    inline void init(uint64_t numElements) { this->numElements = numElements; }

    inline void set(uint32_t startIdx, uint32_t size) {
        this->startIdx = startIdx;
        this->size = size;
    }

    uint32_t getStartIdx() { return startIdx; }
    uint32_t getEndIdx() { return startIdx + size; }
    uint32_t getSize() { return size; }

    bool hasNewRangeToRead();
    inline bool hasValidRangeToRead() { return -1u != startIdx; }

private:
    void reset();

private:
    uint32_t startIdx;
    uint32_t size;
    uint64_t numElements;
};

} // namespace storage
} // namespace graphflow
