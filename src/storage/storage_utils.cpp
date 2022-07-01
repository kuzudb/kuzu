#include "src/storage/include/storage_utils.h"

namespace graphflow {
namespace storage {

uint32_t PageUtils::getNumElementsInAPage(uint32_t elementSize, bool hasNull) {
    auto numBytesPerNullEntry = NullMask::NUM_BITS_PER_NULL_ENTRY >> 3;
    auto numNullEntries =
        hasNull ? (uint32_t)ceil(
                      (double)DEFAULT_PAGE_SIZE /
                      (double)(((uint64_t)elementSize << NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2) +
                               numBytesPerNullEntry)) :
                  0;
    return (DEFAULT_PAGE_SIZE - (numNullEntries * numBytesPerNullEntry)) / elementSize;
}

uint64_t StorageUtils::all1sMaskForLeastSignificantBits(uint64_t numBits) {
    assert(numBits <= 64);
    return numBits == 64 ? UINT64_MAX : (1l << numBits) - 1;
}
} // namespace storage
} // namespace graphflow
