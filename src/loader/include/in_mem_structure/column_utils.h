#pragma once

#include "src/common/include/types.h"
#include "src/storage/include/data_structure/page_utils.h"

using namespace graphflow::storage;

namespace graphflow {
namespace loader {

class ColumnUtils {

public:
    static void calcPageElementCursor(const uint8_t& numBytesPerElement,
        const node_offset_t& nodeOffset, PageElementCursor& cursor);

    static uint64_t calcNumPagesInColumn(uint8_t numBytesPerElement, uint64_t maxElements);
};

} // namespace loader
} // namespace graphflow
