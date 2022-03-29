#include "src/storage/include/storage_structure/overflow_pages.h"

#include "src/common/include/type_utils.h"

namespace graphflow {
namespace storage {

void OverflowPages::readStringsToVector(ValueVector& valueVector) {
    assert(!valueVector.state->isFlat());
    for (auto i = 0u; i < valueVector.state->selectedSize; i++) {
        auto pos = valueVector.state->selectedPositions[i];
        if (!valueVector.isNull(pos)) {
            readStringToVector(
                ((gf_string_t*)valueVector.values)[pos], valueVector.getOverflowBuffer());
        }
    }
}

void OverflowPages::readListsToVector(ValueVector& valueVector) {
    assert(!valueVector.state->isFlat());
    for (auto i = 0u; i < valueVector.state->selectedSize; i++) {
        auto pos = valueVector.state->selectedPositions[i];
        if (!valueVector.isNull(pos)) {
            readListToVector(
                ((gf_list_t*)valueVector.values)[pos], valueVector.getOverflowBuffer());
        }
    }
}

void OverflowPages::readStringToVector(gf_string_t& gfStr, OverflowBuffer& overflowBuffer) {
    PageByteCursor cursor;
    if (!gf_string_t::isShortString(gfStr.len)) {
        TypeUtils::decodeOverflowPtr(gfStr.overflowPtr, cursor.idx, cursor.offset);
        auto frame = bufferManager.pin(fileHandle, cursor.idx);
        TypeUtils::copyString((char*)(frame + cursor.offset), gfStr.len, gfStr, overflowBuffer);
        bufferManager.unpin(fileHandle, cursor.idx);
    }
}

void OverflowPages::readListToVector(gf_list_t& gfList, OverflowBuffer& overflowBuffer) {
    PageByteCursor cursor;
    TypeUtils::decodeOverflowPtr(gfList.overflowPtr, cursor.idx, cursor.offset);
    auto frame = bufferManager.pin(fileHandle, cursor.idx);
    TypeUtils::copyListValues(frame + cursor.offset, gfList, overflowBuffer);
    bufferManager.unpin(fileHandle, cursor.idx);
    if (gfList.childType == STRING) {
        auto gfStrings = (gf_string_t*)(gfList.overflowPtr);
        for (auto i = 0u; i < gfList.size; i++) {
            readStringToVector(gfStrings[i], overflowBuffer);
        }
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
    auto numBytesOfSingleValue = Types::getDataTypeSize(childDataType);
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
