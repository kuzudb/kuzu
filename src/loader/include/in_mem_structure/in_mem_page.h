#pragma once

#include <memory>
#include <vector>

#include "src/common/include/configs.h"
#include "src/common/types/include/node_id_t.h"
#include "src/storage/include/compression_scheme.h"

using namespace graphflow::common;

namespace graphflow {
namespace loader {

class InMemPage {

public:
    // Creates an in-memory page with a boolean array to store NULL bits
    InMemPage(uint32_t maxNumElements, bool hasNULLBytes);

    uint8_t* write(nodeID_t* nodeID, uint32_t byteOffsetInPage, uint32_t elemPosInPage,
        const NodeIDCompressionScheme& compressionScheme);
    uint8_t* write(uint32_t byteOffsetInPage, uint32_t elemPosInPage, const uint8_t* elem,
        uint32_t numBytesForElem);

    void encodeNullBits();

public:
    uint8_t* data;

private:
    uint32_t maxNumElements;
    unique_ptr<uint8_t[]> buffer;
    // Following fields are needed for accommodating storage of NULLs in the page.
    std::unique_ptr<std::vector<bool>> nullMask;
};

} // namespace loader
} // namespace graphflow
