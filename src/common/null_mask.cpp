#include "src/common/include/null_mask.h"

namespace graphflow {

namespace common {

void NullMask::setNull(uint32_t pos, bool isNull) {
    auto entryPos = pos >> NUM_BITS_PER_NULL_ENTRY_LOG2;
    auto bitPosInEntry = pos - (entryPos << NUM_BITS_PER_NULL_ENTRY_LOG2);
    if (isNull) {
        data[entryPos] |= NULL_BITMASKS_WITH_SINGLE_ONE[bitPosInEntry];
        mayContainNulls = true;
    } else {
        data[entryPos] &= NULL_BITMASKS_WITH_SINGLE_ZERO[bitPosInEntry];
    }
}

} // namespace common
} // namespace graphflow
