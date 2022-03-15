#include "src/storage/include/storage_structure/overflow_pages.h"

#include "src/storage/include/storage_structure/storage_structure.h"

namespace graphflow {
namespace storage {

void OverflowPages::readStringsToVector(ValueVector& valueVector) {
    for (auto i = 0u; i < valueVector.state->selectedSize; i++) {
        auto pos = valueVector.state->selectedPositions[i];
        if (!valueVector.isNull(pos)) {
            readStringToVector(
                ((gf_string_t*)valueVector.values)[pos], *valueVector.overflowBuffer);
        }
    }
}

void OverflowPages::readStringToVector(gf_string_t& gfStr, OverflowBuffer& overflowBuffer) {
    PageByteCursor cursor;
    if (!gf_string_t::isShortString(gfStr.len)) {
        TypeUtils::decodeOverflowPtr(gfStr.overflowPtr, cursor.idx, cursor.offset);
        auto frame = bufferManager.pin(fileHandle, cursor.idx);
        overflowBuffer.allocateLargeStringIfNecessary(gfStr, gfStr.len);
        gfStr.set((char*)frame + cursor.offset, gfStr.len);
        bufferManager.unpin(fileHandle, cursor.idx);
    }
}

string OverflowPages::readString(const gf_string_t& str) {
    if (gf_string_t::isShortString(str.len)) {
        return str.getAsShortString();
    } else {
        PageByteCursor cursor;
        TypeUtils::decodeOverflowPtr(str.overflowPtr, cursor.idx, cursor.offset);
        auto frame = bufferManager.pin(fileHandle, cursor.idx);
        auto retVal = string((char*)(frame + cursor.offset), str.len);
        bufferManager.unpin(fileHandle, cursor.idx);
        return retVal;
    }
}

vector<Literal> OverflowPages::readList(const gf_list_t& listVal, DataType childDataType) {
    PageByteCursor cursor;
    TypeUtils::decodeOverflowPtr(listVal.overflowPtr, cursor.idx, cursor.offset);
    auto frame = bufferManager.pin(fileHandle, cursor.idx);
    auto numBytesOfSingleValue = TypeUtils::getDataTypeSize(childDataType);
    auto numValuesInList = listVal.size;
    vector<Literal> retLiterals(numValuesInList);
    if (childDataType == STRING) {
        for (auto i = 0u; i < numValuesInList; i++) {
            auto gfListVal = *(gf_string_t*)(frame + cursor.offset);
            retLiterals[i].strVal = readString(gfListVal);
            retLiterals[i].dataType = childDataType;
            cursor.offset += numBytesOfSingleValue;
        }
    } else {
        for (auto i = 0u; i < numValuesInList; i++) {
            memcpy(&retLiterals[i].val, frame + cursor.offset, numBytesOfSingleValue);
            retLiterals[i].dataType = childDataType;
            cursor.offset += numBytesOfSingleValue;
        }
    }
    bufferManager.unpin(fileHandle, cursor.idx);
    return retLiterals;
}

} // namespace storage
} // namespace graphflow
