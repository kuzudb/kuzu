#pragma once

#include "src/common/types/include/types_include.h"
#include "src/storage/include/storage_structure/storage_structure_utils.h"

using namespace graphflow::storage;

namespace graphflow {
namespace loader {

class ColumnUtils {

public:
    static PageElementCursor calcPageElementCursor(
        const uint8_t& numBytesPerElement, const node_offset_t& nodeOffset);

    static uint64_t calcNumPagesInColumn(uint8_t numBytesPerElement, uint64_t maxElements);
};

} // namespace loader
} // namespace graphflow
