#pragma once

#include "src/storage/include/data_structure/data_structure.h"

using namespace graphflow::storage;

namespace graphflow {
namespace loader {

class ColumnUtils {

public:
    static void calculatePageCursor(
        const uint8_t& numBytesPerElement, const node_offset_t& nodeOffset, PageCursor& cursor);

public:
};

} // namespace loader
} // namespace graphflow
