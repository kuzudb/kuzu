#pragma once

#include "src/common/include/configs.h"
#include "src/storage/include/storage_structure/storage_structure.h"

using namespace graphflow::common;
using namespace graphflow::storage;

namespace graphflow {
namespace loader {

class InMemPage {

public:
    // Creates an in-memory page with a boolean array to store NULL bits
    InMemPage(uint32_t maxElements, uint8_t* pagePointer, bool hasNULLBytes);

    // Writes to pages and also unsets NULL bit
    void write(uint32_t pageOffset, uint32_t posInPage, const uint8_t* value1,
        uint32_t numBytesForValue1, const uint8_t* value2, uint32_t numBytesForValue2);
    void write(
        uint32_t pageOffset, uint32_t posInPage, const uint8_t* value, uint32_t numBytesForValue);

    uint8_t* getPtrToMemLoc(uint32_t pageOffset) { return data + pageOffset; }

    void encodeNULLBytes();

public:
    uint8_t* data;

private:
    // Following fields are needed for accommodating storage of NULLs in the page.
    unique_ptr<bool[]> uncompressedNULLs;
    uint32_t numElementsInPage;
};

} // namespace loader
} // namespace graphflow
