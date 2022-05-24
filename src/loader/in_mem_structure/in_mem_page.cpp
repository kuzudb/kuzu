#include "src/loader/include/in_mem_structure/in_mem_page.h"

#include <algorithm>
#include <cmath>
#include <cstring>

#include "src/storage/include/storage_utils.h"

namespace graphflow {
namespace loader {

InMemPage::InMemPage(uint32_t maxNumElements, bool hasNULLBytes) : maxNumElements{maxNumElements} {
    buffer = make_unique<uint8_t[]>(DEFAULT_PAGE_SIZE);
    data = buffer.get();
    if (hasNULLBytes) {
        // By default, we consider all elements in the page to be NULL. When a new element comes in
        // to be put at a pos, its respective NULL bit is reset to denote a non-NULL value.
        nullMask = std::make_unique<vector<bool>>(maxNumElements, true);
    }
}

uint8_t* InMemPage::write(nodeID_t* nodeID, uint32_t byteOffsetInPage, uint32_t elemPosInPage,
    const NodeIDCompressionScheme& compressionScheme) {
    memcpy(data + byteOffsetInPage, &nodeID->label, compressionScheme.getNumBytesForLabel());
    memcpy(data + byteOffsetInPage + compressionScheme.getNumBytesForLabel(), &nodeID->offset,
        compressionScheme.getNumBytesForOffset());
    if (nullMask) {
        nullMask->operator[](elemPosInPage) = false;
    }
    return data + byteOffsetInPage;
}

uint8_t* InMemPage::write(uint32_t byteOffsetInPage, uint32_t elemPosInPage, const uint8_t* elem,
    uint32_t numBytesForElem) {
    memcpy(data + byteOffsetInPage, elem, numBytesForElem);
    if (nullMask) {
        nullMask->operator[](elemPosInPage) = false;
    }
    return data + byteOffsetInPage;
}

// We hold isNULL value of each element in in_mem_pages in a boolean array. This is packed together
// as bits and the collection of NULLBytes (1 byte has isNULL value of 8 elements) is written to
// the end of the page when writing the page to the disk.
void InMemPage::encodeNullBits() {
    if (!nullMask) {
        return;
    }
    auto pos = 0;
    auto numNULLBytes = ceil((double)maxNumElements / 8);
    for (auto i = 0; i < numNULLBytes; i++) {
        uint8_t NULLByte = 0b0000000;
        for (auto j = 0; j < 8; j++) {
            if (nullMask->operator[](pos)) {
                NULLByte = NULLByte | storage::bitMasksWithSingle1s[pos % 8];
            }
            pos++;
        }
        data[DEFAULT_PAGE_SIZE - 1 - i] = NULLByte;
    }
}

} // namespace loader
} // namespace graphflow
