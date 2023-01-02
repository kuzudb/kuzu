#include "processor/operator/order_by/order_by_key_encoder.h"

#include <string.h>

#include <cmath>
#include <cstdint>

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

OrderByKeyEncoder::OrderByKeyEncoder(vector<shared_ptr<ValueVector>>& orderByVectors,
    vector<bool>& isAscOrder, MemoryManager* memoryManager, uint8_t ftIdx,
    uint32_t numTuplesPerBlockInFT, uint32_t numBytesPerTuple)
    : memoryManager{memoryManager}, orderByVectors{orderByVectors}, isAscOrder{isAscOrder},
      numBytesPerTuple{numBytesPerTuple}, ftIdx{ftIdx},
      numTuplesPerBlockInFT{numTuplesPerBlockInFT}, swapBytes{isLittleEndian()} {
    if (numTuplesPerBlockInFT > MAX_FT_BLOCK_OFFSET) {
        throw RuntimeException(
            "The number of tuples per block of factorizedTable exceeds the maximum blockOffset!");
    }
    keyBlocks.emplace_back(make_unique<DataBlock>(memoryManager));
    assert(this->numBytesPerTuple == getNumBytesPerTuple(orderByVectors));
    maxNumTuplesPerBlock = LARGE_PAGE_SIZE / numBytesPerTuple;
    if (maxNumTuplesPerBlock <= 0) {
        throw RuntimeException(StringUtils::string_format(
            "TupleSize(%d bytes) is larger than the LARGE_PAGE_SIZE(%d bytes)", numBytesPerTuple,
            LARGE_PAGE_SIZE));
    }
    encodeFunctions.resize(orderByVectors.size());
    for (auto i = 0u; i < orderByVectors.size(); i++) {
        encodeFunctions[i] = getEncodingFunction(orderByVectors[i]->dataType.typeID);
    }
}

void OrderByKeyEncoder::encodeKeys() {
    uint32_t numEntries = orderByVectors[0]->state->selVector->selectedSize;
    uint32_t encodedTuples = 0;
    while (numEntries > 0) {
        allocateMemoryIfFull();
        uint32_t numEntriesToEncode =
            min(numEntries, maxNumTuplesPerBlock - getNumTuplesInCurBlock());
        auto tuplePtr =
            keyBlocks.back()->getData() + keyBlocks.back()->numTuples * numBytesPerTuple;
        uint32_t tuplePtrOffset = 0;
        for (auto keyColIdx = 0u; keyColIdx < orderByVectors.size(); keyColIdx++) {
            encodeVector(orderByVectors[keyColIdx], tuplePtr + tuplePtrOffset, encodedTuples,
                numEntriesToEncode, keyColIdx);
            tuplePtrOffset += getEncodingSize(orderByVectors[keyColIdx]->dataType);
        }
        encodeFTIdx(numEntriesToEncode, tuplePtr + tuplePtrOffset);
        encodedTuples += numEntriesToEncode;
        keyBlocks.back()->numTuples += numEntriesToEncode;
        numEntries -= numEntriesToEncode;
    }
}

uint32_t OrderByKeyEncoder::getNumBytesPerTuple(const vector<shared_ptr<ValueVector>>& keyVectors) {
    uint32_t result = 0u;
    for (auto& vector : keyVectors) {
        result += getEncodingSize(vector->dataType);
    }
    result += 8;
    return result;
}

uint32_t OrderByKeyEncoder::getEncodingSize(const DataType& dataType) {
    // Add one more byte for null flag.
    switch (dataType.typeID) {
    case STRING:
        // 1 byte for null flag + 1 byte to indicate long/short string + 12 bytes for string prefix
        return 2 + ku_string_t::SHORT_STR_LENGTH;
    default:
        return 1 + Types::getDataTypeSize(dataType);
    }
}

bool OrderByKeyEncoder::isLittleEndian() {
    // Little endian arch stores the least significant value in the lower bytes.
    int testNumber = 1;
    return *(uint8_t*)&testNumber == 1;
}

void OrderByKeyEncoder::flipBytesIfNecessary(
    uint32_t keyColIdx, uint8_t* tuplePtr, uint32_t numEntriesToEncode, DataType& type) {
    if (!isAscOrder[keyColIdx]) {
        auto encodingSize = getEncodingSize(type);
        // If the current column is in desc order, flip all bytes.
        for (auto i = 0u; i < numEntriesToEncode; i++) {
            for (auto byte = 0u; byte < encodingSize; ++byte) {
                *(tuplePtr + byte) = ~*(tuplePtr + byte);
            }
            tuplePtr += numBytesPerTuple;
        }
    }
}

void OrderByKeyEncoder::encodeFlatVector(
    shared_ptr<ValueVector> vector, uint8_t* tuplePtr, uint32_t keyColIdx) {
    auto pos = vector->state->selVector->selectedPositions[0];
    if (vector->isNull(pos)) {
        for (auto j = 0u; j < getEncodingSize(vector->dataType); j++) {
            *(tuplePtr + j) = UINT8_MAX;
        }
    } else {
        *tuplePtr = 0;
        encodeFunctions[keyColIdx](
            vector->getData() + pos * vector->getNumBytesPerValue(), tuplePtr + 1, swapBytes);
    }
}

void OrderByKeyEncoder::encodeUnflatVector(shared_ptr<ValueVector> vector, uint8_t* tuplePtr,
    uint32_t encodedTuples, uint32_t numEntriesToEncode, uint32_t keyColIdx) {
    if (vector->state->selVector->isUnfiltered()) {
        auto value = vector->getData() + encodedTuples * vector->getNumBytesPerValue();
        if (vector->hasNoNullsGuarantee()) {
            for (auto i = 0u; i < numEntriesToEncode; i++) {
                *tuplePtr = 0;
                encodeFunctions[keyColIdx](value, tuplePtr + 1, swapBytes);
                tuplePtr += numBytesPerTuple;
                value += vector->getNumBytesPerValue();
            }
        } else {
            for (auto i = 0u; i < numEntriesToEncode; i++) {
                if (vector->isNull(encodedTuples + i)) {
                    for (auto j = 0u; j < getEncodingSize(vector->dataType); j++) {
                        *(tuplePtr + j) = UINT8_MAX;
                    }
                } else {
                    *tuplePtr = 0;
                    encodeFunctions[keyColIdx](value, tuplePtr + 1, swapBytes);
                }
                tuplePtr += numBytesPerTuple;
                value += vector->getNumBytesPerValue();
            }
        }
    } else {
        if (vector->hasNoNullsGuarantee()) {
            for (auto i = 0u; i < numEntriesToEncode; i++) {
                *tuplePtr = 0;
                encodeFunctions[keyColIdx](
                    vector->getData() +
                        vector->state->selVector->selectedPositions[i + encodedTuples] *
                            vector->getNumBytesPerValue(),
                    tuplePtr + 1, swapBytes);
                tuplePtr += numBytesPerTuple;
            }
        } else {
            for (auto i = 0u; i < numEntriesToEncode; i++) {
                auto pos = vector->state->selVector->selectedPositions[i + encodedTuples];
                if (vector->isNull(pos)) {
                    for (auto j = 0u; j < getEncodingSize(vector->dataType); j++) {
                        *(tuplePtr + j) = UINT8_MAX;
                    }
                } else {
                    *tuplePtr = 0;
                    encodeFunctions[keyColIdx](
                        vector->getData() + pos * vector->getNumBytesPerValue(), tuplePtr + 1,
                        swapBytes);
                }
                tuplePtr += numBytesPerTuple;
            }
        }
    }
}

void OrderByKeyEncoder::encodeVector(shared_ptr<ValueVector> vector, uint8_t* tuplePtr,
    uint32_t encodedTuples, uint32_t numEntriesToEncode, uint32_t keyColIdx) {
    if (vector->state->isFlat()) {
        encodeFlatVector(vector, tuplePtr, keyColIdx);
    } else {
        encodeUnflatVector(vector, tuplePtr, encodedTuples, numEntriesToEncode, keyColIdx);
    }
    flipBytesIfNecessary(keyColIdx, tuplePtr, numEntriesToEncode, vector->dataType);
}

void OrderByKeyEncoder::encodeFTIdx(uint32_t numEntriesToEncode, uint8_t* tupleInfoPtr) {
    uint32_t numUpdatedFTInfoEntries = 0;
    while (numUpdatedFTInfoEntries < numEntriesToEncode) {
        auto nextBatchOfEntries = min(
            numEntriesToEncode - numUpdatedFTInfoEntries, numTuplesPerBlockInFT - ftBlockOffset);
        for (auto i = 0u; i < nextBatchOfEntries; i++) {
            *(uint32_t*)tupleInfoPtr = ftBlockIdx;
            *(uint32_t*)(tupleInfoPtr + 4) = ftBlockOffset;
            *(uint8_t*)(tupleInfoPtr + 7) = ftIdx;
            tupleInfoPtr += numBytesPerTuple;
            ftBlockOffset++;
        }
        numUpdatedFTInfoEntries += nextBatchOfEntries;
        if (ftBlockOffset == numTuplesPerBlockInFT) {
            ftBlockIdx++;
            ftBlockOffset = 0;
        }
    }
}

void OrderByKeyEncoder::allocateMemoryIfFull() {
    if (getNumTuplesInCurBlock() == maxNumTuplesPerBlock) {
        keyBlocks.emplace_back(make_shared<DataBlock>(memoryManager));
    }
}

encode_function_t OrderByKeyEncoder::getEncodingFunction(DataTypeID typeId) {
    switch (typeId) {
    case BOOL: {
        return encodeTemplate<bool>;
    }
    case INT64: {
        return encodeTemplate<int64_t>;
    }
    case DOUBLE: {
        return encodeTemplate<double>;
    }
    case STRING: {
        return encodeTemplate<ku_string_t>;
    }
    case DATE: {
        return encodeTemplate<date_t>;
    }
    case TIMESTAMP: {
        return encodeTemplate<timestamp_t>;
    }
    case INTERVAL: {
        return encodeTemplate<interval_t>;
    }
    default: {
        throw RuntimeException("Cannot encode data type " + Types::dataTypeToString(typeId));
    }
    }
}

template<>
void OrderByKeyEncoder::encodeData(int32_t data, uint8_t* resultPtr, bool swapBytes) {
    if (swapBytes) {
        data = BSWAP32(data);
    }
    memcpy(resultPtr, (void*)&data, sizeof(data));
    resultPtr[0] = flipSign(resultPtr[0]);
}

template<>
void OrderByKeyEncoder::encodeData(int64_t data, uint8_t* resultPtr, bool swapBytes) {
    if (swapBytes) {
        data = BSWAP64(data);
    }
    memcpy(resultPtr, (void*)&data, sizeof(data));
    resultPtr[0] = flipSign(resultPtr[0]);
}

template<>
void OrderByKeyEncoder::encodeData(bool data, uint8_t* resultPtr, bool swapBytes) {
    uint8_t val = data ? 1 : 0;
    memcpy(resultPtr, (void*)&val, sizeof(data));
}

template<>
void OrderByKeyEncoder::encodeData(double data, uint8_t* resultPtr, bool swapBytes) {
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

template<>
void OrderByKeyEncoder::encodeData(date_t data, uint8_t* resultPtr, bool swapBytes) {
    encodeData(data.days, resultPtr, swapBytes);
}

template<>
void OrderByKeyEncoder::encodeData(timestamp_t data, uint8_t* resultPtr, bool swapBytes) {
    encodeData(data.value, resultPtr, swapBytes);
}

template<>
void OrderByKeyEncoder::encodeData(interval_t data, uint8_t* resultPtr, bool swapBytes) {
    int64_t months, days, micros;
    Interval::NormalizeIntervalEntries(data, months, days, micros);
    encodeData((int32_t)months, resultPtr, swapBytes);
    resultPtr += sizeof(data.months);
    encodeData((int32_t)days, resultPtr, swapBytes);
    resultPtr += sizeof(data.days);
    encodeData(micros, resultPtr, swapBytes);
}

template<>
void OrderByKeyEncoder::encodeData(ku_string_t data, uint8_t* resultPtr, bool swapBytes) {
    // Only encode the prefix of ku_string.
    memcpy(resultPtr, (void*)data.getAsString().c_str(),
        min((uint32_t)ku_string_t::SHORT_STR_LENGTH, data.len));
    if (ku_string_t::isShortString(data.len)) {
        memset(resultPtr + data.len, '\0', ku_string_t::SHORT_STR_LENGTH + 1 - data.len);
    } else {
        resultPtr[12] = UINT8_MAX;
    }
}

} // namespace processor
} // namespace kuzu
