#include "storage/storage_structure/in_mem_page.h"

#include <cstring>

#include "common/constants.h"
#include "common/null_mask.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

InMemPage::InMemPage()
    : InMemPage{BufferPoolConstants::PAGE_4KB_SIZE, 1 /* numBytesForElement */,
          false /* hasNullEntries */} {}

InMemPage::InMemPage(uint32_t maxNumElements, uint16_t numBytesForElement, bool hasNullEntries)
    : nullEntriesInPage{nullptr}, maxNumElements{maxNumElements} {
    buffer = std::make_unique<uint8_t[]>(BufferPoolConstants::PAGE_4KB_SIZE);
    data = buffer.get();
    if (hasNullEntries) {
        // In a page, null entries are stored right after the element data. Each null entry contains
        // 64 bits, and one bit indicates an element with corresponding position in this page is
        // null or not. By default, we consider all elements in the page to be NULL. When a new
        // element comes in to be put at a pos, its respective NULL bit is reset to denote a
        // non-NULL value.
        nullEntriesInPage = (uint64_t*)(data + (numBytesForElement * maxNumElements));
        auto numNullEntries = (maxNumElements + NullMask::NUM_BITS_PER_NULL_ENTRY - 1) /
                              NullMask::NUM_BITS_PER_NULL_ENTRY;
        std::fill(nullEntriesInPage, nullEntriesInPage + numNullEntries, NullMask::ALL_NULL_ENTRY);
        nullMask = std::make_unique<uint8_t[]>(maxNumElements);
        memset(nullMask.get(), UINT8_MAX, maxNumElements);
    }
}

void InMemPage::setElementAtPosToNonNull(uint32_t pos) {
    auto entryPos = pos >> NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2;
    auto bitPosInEntry = pos - (entryPos << NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2);
    nullEntriesInPage[entryPos] &= NULL_BITMASKS_WITH_SINGLE_ZERO[bitPosInEntry];
}

uint8_t* InMemPage::write(uint32_t byteOffsetInPage, uint32_t elemPosInPage, const uint8_t* elem,
    uint32_t numBytesForElem) {
    memcpy(data + byteOffsetInPage, elem, numBytesForElem);
    if (nullMask) {
        nullMask[elemPosInPage] = false;
    }
    return data + byteOffsetInPage;
}

void InMemPage::encodeNullBits() {
    if (nullMask == nullptr) {
        return;
    }
    for (auto i = 0u; i < maxNumElements; i++) {
        if (!nullMask[i]) {
            setElementAtPosToNonNull(i);
        }
    }
}

} // namespace storage
} // namespace kuzu
