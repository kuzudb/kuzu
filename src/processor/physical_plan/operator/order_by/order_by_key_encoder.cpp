#include "src/processor/include/physical_plan/operator/order_by/order_by_key_encoder.h"

#include <string.h>

#include <cmath>
#include <cstdint>

#include "src/common/include/assert.h"
#include "src/common/include/interval.h"

#define BSWAP64(x)                                                                                 \
    ((uint64_t)((((uint64_t)(x)&0xff00000000000000ull) >> 56) |                                    \
                (((uint64_t)(x)&0x00ff000000000000ull) >> 40) |                                    \
                (((uint64_t)(x)&0x0000ff0000000000ull) >> 24) |                                    \
                (((uint64_t)(x)&0x000000ff00000000ull) >> 8) |                                     \
                (((uint64_t)(x)&0x00000000ff000000ull) << 8) |                                     \
                (((uint64_t)(x)&0x0000000000ff0000ull) << 24) |                                    \
                (((uint64_t)(x)&0x000000000000ff00ull) << 40) |                                    \
                (((uint64_t)(x)&0x00000000000000ffull) << 56)))

#define BSWAP32(x)                                                                                 \
    ((uint32_t)((((uint32_t)(x)&0xff000000) >> 24) | (((uint32_t)(x)&0x00ff0000) >> 8) |           \
                (((uint32_t)(x)&0x0000ff00) << 8) | (((uint32_t)(x)&0x000000ff) << 24)))

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace processor {

uint8_t OrderByKeyEncoder::flipSign(uint8_t key_byte) {
    return key_byte ^ 128;
}

bool OrderByKeyEncoder::isLittleEndian() {
    // little endian arch stores the least significant value in the lower bytes
    int testNumber = 1;
    return *(uint8_t*)&testNumber == 1;
}

void OrderByKeyEncoder::encodeInt32(int32_t data, uint8_t* resultPtr) {
    if (isLittleEndian()) {
        data = BSWAP32(data);
    }
    memcpy(resultPtr, (void*)&data, sizeof(data));
    resultPtr[0] = flipSign(resultPtr[0]);
}

void OrderByKeyEncoder::encodeInt64(int64_t data, uint8_t* resultPtr) {
    if (isLittleEndian()) {
        data = BSWAP64(data);
    }
    memcpy(resultPtr, (void*)&data, sizeof(data));
    resultPtr[0] = flipSign(resultPtr[0]);
}

void OrderByKeyEncoder::encodeBool(bool data, uint8_t* resultPtr) {
    uint8_t val = data ? 1 : 0;
    memcpy(resultPtr, (void*)&val, sizeof(data));
}

void OrderByKeyEncoder::encodeDouble(double data, uint8_t* resultPtr) {
    memcpy(resultPtr, &data, sizeof(data));
    uint64_t* dataBytes = (uint64_t*)resultPtr;
    if (isLittleEndian()) {
        *dataBytes = BSWAP64(*dataBytes);
    }
    if (data < (double)0) {
        *dataBytes = ~*dataBytes;
    } else {
        resultPtr[0] = flipSign(resultPtr[0]);
    }
}

void OrderByKeyEncoder::encodeDate(date_t data, uint8_t* resultPtr) {
    encodeInt32(data.days, resultPtr);
}

void OrderByKeyEncoder::encodeTimestamp(timestamp_t data, uint8_t* resultPtr) {
    encodeInt64(data.value, resultPtr);
}

void OrderByKeyEncoder::encodeInterval(interval_t data, uint8_t* resultPtr) {
    int64_t months, days, micros;
    Interval::NormalizeIntervalEntries(data, months, days, micros);
    encodeInt32(months, resultPtr);
    resultPtr += sizeof(data.months);
    encodeInt32(days, resultPtr);
    resultPtr += sizeof(data.days);
    encodeInt64(micros, resultPtr);
}

void OrderByKeyEncoder::encodeString(gf_string_t data, uint8_t* resultPtr) {
    // Only encode the prefix of gf_string.
    memcpy(resultPtr, (void*)data.getAsString().c_str(),
        min((uint32_t)gf_string_t::SHORT_STR_LENGTH, data.len));
    if (data.len < (uint32_t)gf_string_t::SHORT_STR_LENGTH) {
        memset(resultPtr + data.len, '\0', gf_string_t::SHORT_STR_LENGTH - data.len);
    }
}

uint64_t OrderByKeyEncoder::getEncodingSize(DataType dataType) {
    // Add one more byte for null flag.
    return 1 + (dataType == STRING ? gf_string_t::SHORT_STR_LENGTH :
                                     TypeUtils::getDataTypeSize(dataType));
}

void OrderByKeyEncoder::allocateMemoryIfFull() {
    if (curBlockUsedEntries == entriesPerBlock) {
        keyBlocks.emplace_back(memoryManager.allocateBlock(SORT_BLOCK_SIZE));
        curBlockUsedEntries = 0;
    }
}

void OrderByKeyEncoder::encodeData(shared_ptr<ValueVector>& orderByVector,
    uint64_t idxInOrderByVector, uint8_t* keyBlockPtr, const uint64_t keyColIdx) {
    if (orderByVector->isNull(idxInOrderByVector)) {
        // Set all bits to 1 if this is a null value.
        for (uint64_t i = 0; i < getEncodingSize(orderByVector->dataType); i++) {
            *(keyBlockPtr + i) = UINT8_MAX;
        }
    } else {
        *keyBlockPtr = 0; // Set the null flag to 0.
        auto keyBlockPtrAfterNullByte = keyBlockPtr + 1;
        switch (orderByVector->dataType) {
        case INT64:
            encodeInt64(
                ((int64_t*)orderByVector->values)[idxInOrderByVector], keyBlockPtrAfterNullByte);
            break;
        case BOOL:
            encodeBool(
                ((bool*)orderByVector->values)[idxInOrderByVector], keyBlockPtrAfterNullByte);
            break;
        case DOUBLE:
            encodeDouble(
                ((double*)orderByVector->values)[idxInOrderByVector], keyBlockPtrAfterNullByte);
            break;
        case DATE:
            encodeDate(
                ((date_t*)orderByVector->values)[idxInOrderByVector], keyBlockPtrAfterNullByte);
            break;
        case TIMESTAMP:
            encodeTimestamp(((timestamp_t*)orderByVector->values)[idxInOrderByVector],
                keyBlockPtrAfterNullByte);
            break;
        case INTERVAL:
            encodeInterval(
                ((interval_t*)orderByVector->values)[idxInOrderByVector], keyBlockPtrAfterNullByte);
            break;
        case STRING:
            encodeString(((gf_string_t*)orderByVector->values)[idxInOrderByVector],
                keyBlockPtrAfterNullByte);
            break;
        default:
            throw EncodingException(
                "Unimplemented datatype: " + TypeUtils::dataTypeToString(orderByVector->dataType));
        }
    }
    if (!isAscOrder[keyColIdx]) {
        // If the current column is in desc order, flip all bits.
        for (uint64_t byte = 0; byte < getEncodingSize(orderByVector->dataType); ++byte) {
            *(keyBlockPtr + byte) = ~*(keyBlockPtr + byte);
        }
    }
}

void OrderByKeyEncoder::encodeKeys() {
    uint64_t numEntries = 0;
    if (orderByVectors[0]->state->isFlat()) {
        numEntries = 1;
    } else {
        // We only allow an input key vector to be unflat if there is a single order by column.
        GF_ASSERT(orderByVectors.size() == 1);
        numEntries = orderByVectors[0]->state->selectedSize;
    }
    // Check whether the nextLocalRowID overflows.
    if (nextLocalRowID + numEntries - 1 > MAX_LOCAL_ROW_ID) {
        throw EncodingException("Attempting to order too many rows. The orderByKeyEncoder has "
                                "achieved its maximum number of rows!");
    }
    uint64_t encodedRows = 0;
    while (numEntries > 0) {
        allocateMemoryIfFull();
        uint64_t numEntriesToEncode = min(numEntries, entriesPerBlock - curBlockUsedEntries);
        for (uint64_t i = 0; i < numEntriesToEncode; i++) {
            uint64_t keyBlockPtrOffset = 0;
            const auto basePtr =
                keyBlocks.back()->data + curBlockUsedEntries * keyBlockEntrySizeInBytes;
            for (uint64_t keyColIdx = 0; keyColIdx < orderByVectors.size(); keyColIdx++) {
                auto const keyBlockPtr = basePtr + keyBlockPtrOffset;
                uint64_t idxInOrderByVector =
                    orderByVectors[0]->state->isFlat() ?
                        orderByVectors[keyColIdx]->state->getPositionOfCurrIdx() :
                        encodedRows;
                encodeData(orderByVectors[keyColIdx], idxInOrderByVector, keyBlockPtr, keyColIdx);
                keyBlockPtrOffset += getEncodingSize(orderByVectors[keyColIdx]->dataType);
            }
            // Since only the lower 6 bytes of nextLocalRowID is actually used, we only need to
            // encode the lower 6 bytes.
            memcpy(basePtr + keyBlockPtrOffset, &nextLocalRowID, getEncodedRowIDSizeInBytes());
            // 2 bytes encoding for threadID
            memcpy(basePtr + keyBlockPtrOffset + getEncodedRowIDSizeInBytes(), &threadID,
                sizeof(threadID));
            encodedRows++;
            curBlockUsedEntries++;
            nextLocalRowID++;
        }
        numEntries -= numEntriesToEncode;
    }
}

} // namespace processor
} // namespace graphflow
