#include "src/loader/include/in_mem_structure/in_mem_page.h"

#include <algorithm>
#include <cmath>
#include <cstring>

namespace graphflow {
namespace loader {

InMemPage::InMemPage(uint32_t maxElements, uint8_t* pagePointer, bool hasNULLBytes) {
    numElementsInPage = maxElements;
    data = pagePointer;
    if (hasNULLBytes) {
        uncompressedNULLs = std::make_unique<bool[]>(maxElements);
        // By default, we consider all elements in the page to be NULL. When a new element comes in
        // to be put at a pos, its respective NULL bit is reset to denote a non-NULL value.
        std::fill(uncompressedNULLs.get(), uncompressedNULLs.get() + maxElements, 1);
    } else {
        uncompressedNULLs = nullptr;
    }
}

void InMemPage::write(uint32_t pageOffset, uint32_t posInPage, const uint8_t* value1,
    uint32_t numBytesForValue1, const uint8_t* value2, uint32_t numBytesForValue2) {
    auto writePtr = getPtrToMemLoc(pageOffset);
    memcpy(writePtr, value1, numBytesForValue1);
    memcpy(writePtr + numBytesForValue1, value2, numBytesForValue2);
    if (uncompressedNULLs) {
        uncompressedNULLs[posInPage] = false;
    }
}

uint8_t* InMemPage::write(
    uint32_t pageOffset, uint32_t posInPage, const uint8_t* value, uint32_t numBytesForValue) {
    auto writePtr = getPtrToMemLoc(pageOffset);
    memcpy(writePtr, value, numBytesForValue);
    if (uncompressedNULLs) {
        uncompressedNULLs[posInPage] = false;
    }
    return writePtr;
}

// We hold isNULL value of each element in in_mem_pages in a boolean array. This is packed together
// as bits and the collection of NULLBytes (1 byte has isNULL value of 8 elements) is written to
// the end of the page when writing the page to the disk.
void InMemPage::encodeNULLBytes() {
    if (!uncompressedNULLs) {
        return;
    }
    auto pos = 0;
    auto numNULLBytes = ceil((double)numElementsInPage / 8);
    for (auto i = 0; i < numNULLBytes; i++) {
        uint8_t NULLByte = 0b0000000;
        for (auto j = 0; j < 8; j++) {
            if (uncompressedNULLs[pos]) {
                switch (pos % 8) {
                case 0:
                    NULLByte = NULLByte | 0b10000000;
                    break;
                case 1:
                    NULLByte = NULLByte | 0b01000000;
                    break;
                case 2:
                    NULLByte = NULLByte | 0b00100000;
                    break;
                case 3:
                    NULLByte = NULLByte | 0b00010000;
                    break;
                case 4:
                    NULLByte = NULLByte | 0b00001000;
                    break;
                case 5:
                    NULLByte = NULLByte | 0b00000100;
                    break;
                case 6:
                    NULLByte = NULLByte | 0b00000010;
                    break;
                case 7:
                    NULLByte = NULLByte | 0b00000001;
                    break;
                }
            }
            pos++;
        }
        data[DEFAULT_PAGE_SIZE - 1 - i] = NULLByte;
    }
}

} // namespace loader
} // namespace graphflow
