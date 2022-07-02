#pragma once

#include <memory>
#include <vector>

#include "src/common/include/configs.h"
#include "src/common/include/null_mask.h"
#include "src/common/types/include/node_id_t.h"
#include "src/storage/include/compression_scheme.h"

using namespace graphflow::common;

namespace graphflow {
namespace loader {

class InMemPage {

public:
    // Creates an in-memory page with a boolean array to store NULL bits
    InMemPage(uint32_t maxNumElements, uint16_t numBytesForElement, bool hasNullEntries);

    uint8_t* write(nodeID_t* nodeID, uint32_t byteOffsetInPage, uint32_t elemPosInPage,
        const NodeIDCompressionScheme& compressionScheme);
    uint8_t* write(uint32_t byteOffsetInPage, uint32_t elemPosInPage, const uint8_t* elem,
        uint32_t numBytesForElem);

    void encodeNullBits();

public:
    uint8_t* data;

private:
    void setElementAtPosToNonNull(uint32_t pos);

    unique_ptr<uint8_t[]> buffer;
    // The pointer to the beginning of null entries in the page.
    uint64_t* nullEntriesInPage;
    unique_ptr<uint8_t[]> nullMask;
    uint32_t maxNumElements;
};

} // namespace loader
} // namespace graphflow
