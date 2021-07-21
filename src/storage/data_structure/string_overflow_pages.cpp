#include "src/storage/include/data_structure/string_overflow_pages.h"

#include "src/storage/include/data_structure/data_structure.h"

namespace graphflow {
namespace storage {

void StringOverflowPages::readStringsToVector(
    ValueVector& valueVector, BufferManagerMetrics& metrics) {
    for (auto i = 0u; i < valueVector.state->size; i++) {
        auto pos = valueVector.state->selectedValuesPos[i];
        readStringToVector(valueVector, pos, metrics);
    }
}

void StringOverflowPages::readStringToVector(
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

string StringOverflowPages::readString(const gf_string_t& str, BufferManagerMetrics& metrics) {
    if (str.len <= gf_string_t::SHORT_STR_LENGTH) {
        return str.getAsShortString();
    } else {
        PageCursor cursor;
        str.getOverflowPtrInfo(cursor.idx, cursor.offset);
        auto frame = bufferManager.pin(fileHandle, cursor.idx, metrics);
        auto retVal = string((char*)(frame + cursor.offset), str.len);
        bufferManager.unpin(fileHandle, cursor.idx);
        return retVal;
    }
}

} // namespace storage
} // namespace graphflow
