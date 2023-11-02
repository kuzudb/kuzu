#pragma once

#include <cstdint>
#include <memory>

namespace kuzu {
namespace storage {

class InMemPage {

public:
    explicit InMemPage();
    // Creates an in-memory page with a boolean array to store NULL bits
    InMemPage(uint32_t maxNumElements, uint16_t numBytesForElement, bool hasNullEntries);

    uint8_t* write(uint32_t byteOffsetInPage, uint32_t elemPosInPage, const uint8_t* elem,
        uint32_t numBytesForElem);

    void encodeNullBits();

public:
    uint8_t* data;

private:
    void setElementAtPosToNonNull(uint32_t pos);

    std::unique_ptr<uint8_t[]> buffer;
    // The pointer to the beginning of null entries in the page.
    uint64_t* nullEntriesInPage;
    std::unique_ptr<uint8_t[]> nullMask;
    uint32_t maxNumElements;
};

} // namespace storage
} // namespace kuzu
