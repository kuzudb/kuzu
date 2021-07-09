#include "src/storage/include/data_structure/string_overflow_pages.h"

#include "src/storage/include/data_structure/utils.h"

namespace graphflow {
namespace storage {

void StringOverflowPages::readStringsFromOverflowPages(
    ValueVector& valueVector, BufferManagerMetrics& metrics) {
    for (auto i = 0u; i < valueVector.state->size; i++) {
        auto pos = valueVector.state->selectedValuesPos[i];
        readAStringFromOverflowPages(valueVector, pos, metrics);
    }
}

void StringOverflowPages::readAStringFromOverflowPages(
    ValueVector& valueVector, uint32_t pos, BufferManagerMetrics& metrics) {
    PageCursor cursor;
    auto& value = ((gf_string_t*)valueVector.values)[pos];
    if (value.len > gf_string_t::SHORT_STR_LENGTH) {
        value.getOverflowPtrInfo(cursor.idx, cursor.offset);
        auto frame = bufferManager.pin(fileHandle, cursor.idx, metrics);
        valueVector.addString(pos, (char*)frame + cursor.offset, value.len);
        bufferManager.unpin(fileHandle, cursor.idx);
    }
}

} // namespace storage
} // namespace graphflow
