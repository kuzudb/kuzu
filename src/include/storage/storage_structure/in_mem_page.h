#pragma once

#include <memory>
#include <vector>

#include "common/configs.h"
#include "common/null_mask.h"
#include "common/types/node_id_t.h"
#include "storage/node_id_compression_scheme.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

class InMemPage {

public:
    // Creates an in-memory page with a boolean array to store NULL bits
    InMemPage(uint32_t maxNumElements, uint16_t numBytesForElement, bool hasNullEntries);

    inline bool isElemPosNull(uint16_t elemPosInPage) const { return nullMask[elemPosInPage]; }

    uint8_t* writeNodeID(nodeID_t* nodeID, uint32_t byteOffsetInPage, uint32_t elemPosInPage,
        const NodeIDCompressionScheme& nodeIDCompressionScheme);
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

} // namespace storage
} // namespace kuzu
