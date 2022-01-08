#include "src/storage/include/data_structure/string_overflow_pages.h"

#include "src/storage/include/data_structure/data_structure.h"

namespace graphflow {
namespace storage {

void StringOverflowPages::readStringsToVector(
    ValueVector& valueVector, BufferManagerMetrics& metrics) {
    for (auto i = 0u; i < valueVector.state->selectedSize; i++) {
        auto pos = valueVector.state->selectedPositions[i];
        if (!valueVector.isNull(pos)) {
            readStringToVector(
                ((gf_string_t*)valueVector.values)[pos], *valueVector.stringBuffer, metrics);
        }
    }
}

void StringOverflowPages::readStringToVector(
    gf_string_t& gfStr, StringBuffer& stringBuffer, BufferManagerMetrics& metrics) {
    PageByteCursor cursor;
    if (!gf_string_t::isShortString(gfStr.len)) {
        gfStr.getOverflowPtrInfo(cursor.idx, cursor.offset);
        auto frame = bufferManager.pin(fileHandle, cursor.idx, metrics);
        stringBuffer.allocateLargeStringIfNecessary(gfStr, gfStr.len);
        gfStr.set((char*)frame + cursor.offset, gfStr.len);
        bufferManager.unpin(fileHandle, cursor.idx);
    }
}

string StringOverflowPages::readString(const gf_string_t& str, BufferManagerMetrics& metrics) {
    if (gf_string_t::isShortString(str.len)) {
        return str.getAsShortString();
    } else {
        PageByteCursor cursor;
        str.getOverflowPtrInfo(cursor.idx, cursor.offset);
        auto frame = bufferManager.pin(fileHandle, cursor.idx, metrics);
        auto retVal = string((char*)(frame + cursor.offset), str.len);
        bufferManager.unpin(fileHandle, cursor.idx);
        return retVal;
    }
}

} // namespace storage
} // namespace graphflow
