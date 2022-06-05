#include "src/processor/include/physical_plan/operator/order_by/order_by_key_encoder.h"

#include <string.h>

#include <cmath>
#include <cstdint>

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

OrderByKeyEncoder::OrderByKeyEncoder(vector<shared_ptr<ValueVector>>& orderByVectors,
    vector<bool>& isAscOrder, MemoryManager* memoryManager, uint8_t ftIdx,
    uint32_t numTuplesPerBlockInFT)
    : memoryManager{memoryManager}, orderByVectors{orderByVectors}, isAscOrder{isAscOrder},
      ftIdx{ftIdx}, numTuplesPerBlockInFT{numTuplesPerBlockInFT}, swapBytes{isLittleEndian()} {
    if (numTuplesPerBlockInFT > MAX_FT_BLOCK_OFFSET) {
        throw RuntimeException(
            "The number of tuples per block of factorizedTable exceeds the maximum blockOffset!");
    }
    keyBlocks.emplace_back(make_unique<DataBlock>(memoryManager));
    numBytesPerTuple = 0;
    for (auto& orderByVector : orderByVectors) {
        numBytesPerTuple += getEncodingSize(orderByVector->dataType);
    }
    numBytesPerTuple += 8;
    maxNumTuplesPerBlock = LARGE_PAGE_SIZE / numBytesPerTuple;
    if (maxNumTuplesPerBlock <= 0) {
        throw RuntimeException(StringUtils::string_format(
            "TupleSize(%d bytes) is larger than the LARGE_PAGE_SIZE(%d bytes)", numBytesPerTuple,
            LARGE_PAGE_SIZE));
    }
}

uint32_t OrderByKeyEncoder::getEncodingSize(const DataType& dataType) {
    // Add one more byte for null flag.
    switch (dataType.typeID) {
    case UNSTRUCTURED:
        // 1 byte for null flag + 1 byte for unstr data
        return 2;
    case STRING:
        // 1 byte for null flag + 1 byte to indicate long/short string + 12 bytes for string prefix
        return 2 + gf_string_t::SHORT_STR_LENGTH;
    default:
        return 1 + Types::getDataTypeSize(dataType);
    }
}

void OrderByKeyEncoder::encodeKeys() {
    uint32_t numEntries =
        orderByVectors[0]->state->isFlat() ? 1 : orderByVectors[0]->state->selectedSize;
    uint32_t encodedTuples = 0;
    while (numEntries > 0) {
        allocateMemoryIfFull();
        uint32_t numEntriesToEncode =
            min(numEntries, maxNumTuplesPerBlock - getNumTuplesInCurBlock());
        auto tuplePtr =
            keyBlocks.back()->getData() + keyBlocks.back()->numTuples * numBytesPerTuple;
        uint32_t numEntriesEncoded = 0;
        while (numEntriesEncoded < numEntriesToEncode) {
            auto nextBatchOfEntriesToEncode =
                min(numEntriesToEncode - numEntriesEncoded, numTuplesPerBlockInFT - ftBlockOffset);
            for (auto i = 0u; i < nextBatchOfEntriesToEncode; i++) {
                uint32_t tuplePtrOffset = 0;
                for (auto keyColIdx = 0u; keyColIdx < orderByVectors.size(); keyColIdx++) {
                    auto const keyBlockPtr = tuplePtr + tuplePtrOffset;
                    uint32_t idxInOrderByVector =
                        orderByVectors[0]->state->isFlat() ?
                            orderByVectors[keyColIdx]->state->getPositionOfCurrIdx() :
                            orderByVectors[keyColIdx]->state->selectedPositions[encodedTuples];
                    encodeData(
                        orderByVectors[keyColIdx], idxInOrderByVector, keyBlockPtr, keyColIdx);
                    tuplePtrOffset += getEncodingSize(orderByVectors[keyColIdx]->dataType);
                }
                auto tupleInfoPtr = tuplePtr + tuplePtrOffset;
                *(uint32_t*)tupleInfoPtr = ftBlockIdx;
                *(uint32_t*)(tupleInfoPtr + 4) = ftBlockOffset;
                *(uint8_t*)(tupleInfoPtr + 7) = ftIdx;
                encodedTuples++;
                tuplePtr += numBytesPerTuple;
                ftBlockOffset++;
            }
            if (ftBlockOffset == numTuplesPerBlockInFT) {
                ftBlockIdx++;
                ftBlockOffset = 0;
            }
            numEntriesEncoded += nextBatchOfEntriesToEncode;
        }
        keyBlocks.back()->numTuples += numEntriesToEncode;
        numEntries -= numEntriesToEncode;
    }
}

bool OrderByKeyEncoder::isLittleEndian() {
    // Little endian arch stores the least significant value in the lower bytes.
    int testNumber = 1;
    return *(uint8_t*)&testNumber == 1;
}

void OrderByKeyEncoder::encodeInt32(int32_t data, uint8_t* resultPtr) {
    if (swapBytes) {
        data = BSWAP32(data);
    }
    memcpy(resultPtr, (void*)&data, sizeof(data));
    resultPtr[0] = flipSign(resultPtr[0]);
}

void OrderByKeyEncoder::encodeInt64(int64_t data, uint8_t* resultPtr) {
    if (swapBytes) {
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
    if (swapBytes) {
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
    if (gf_string_t::isShortString(data.len)) {
        memset(resultPtr + data.len, '\0', gf_string_t::SHORT_STR_LENGTH + 1 - data.len);
    } else {
        resultPtr[12] = UINT8_MAX;
    }
}

void OrderByKeyEncoder::encodeUnstr(uint8_t* resultPtr) {
    // Encode all unstr data as 0, meaning that there is a tie for the entire column
    uint8_t encodeVal = 0;
    memcpy(resultPtr, &encodeVal, sizeof(encodeVal));
}

void OrderByKeyEncoder::allocateMemoryIfFull() {
    if (getNumTuplesInCurBlock() == maxNumTuplesPerBlock) {
        keyBlocks.emplace_back(make_shared<DataBlock>(memoryManager));
    }
}

void OrderByKeyEncoder::encodeData(shared_ptr<ValueVector>& orderByVector,
    uint32_t idxInOrderByVector, uint8_t* keyBlockPtr, uint32_t keyColIdx) {
    if (orderByVector->isNull(idxInOrderByVector)) {
        // Set all bits to 1 if this is a null value.
        for (auto i = 0u; i < getEncodingSize(orderByVector->dataType); i++) {
            *(keyBlockPtr + i) = UINT8_MAX;
        }
    } else {
        *keyBlockPtr = 0; // Set the null flag to 0.
        auto keyBlockPtrAfterNullByte = keyBlockPtr + 1;
        switch (orderByVector->dataType.typeID) {
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
        case UNSTRUCTURED:
            encodeUnstr(keyBlockPtrAfterNullByte);
            break;
        default:
            throw RuntimeException("Unimplemented datatype: " +
                                   Types::dataTypeToString(orderByVector->dataType.typeID));
        }
    }
    if (!isAscOrder[keyColIdx]) {
        // If the current column is in desc order, flip all bytes.
        for (auto byte = 0u; byte < getEncodingSize(orderByVector->dataType); ++byte) {
            *(keyBlockPtr + byte) = ~*(keyBlockPtr + byte);
        }
    }
}

} // namespace processor
} // namespace graphflow
