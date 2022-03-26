#pragma once

#include <string>
#include <vector>

#include "src/common/include/configs.h"
#include "src/common/include/exception.h"
#include "src/common/include/utils.h"
#include "src/common/include/vector/value_vector.h"
#include "src/processor/include/physical_plan/result/factorized_table.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace processor {

// The OrderByKeyEncoder encodes all columns in the ORDER BY clause into a single binary sequence
// that, when compared using memcmp will yield the correct overall sorting order. On little-endian
// hardware, the least-significant byte is stored at the smallest address. To encode the sorting
// order, we need the big-endian representation for values. For example: we want to encode 73(INT64)
// and 38(INT64) as an 8-byte binary string The encoding in little-endian hardware is:
// 73=0x4900000000000000 38=0x2600000000000000, which doesn't preserve the order. The encoding in
// big-endian hardware is: 73=0x0000000000000049 38=0x0000000000000026, which can easily be compared
// using memcmp. In addition, The first bit is also flipped to preserve ordering between positive
// and negative numbers. So the final encoding for 73(INT64) and 38(INT64) as an 8-byte binary
// string is: 73=0x8000000000000049 38=0x8000000000000026. To handle the null in comparison, we
// add an extra byte(called the NULL flag) to represent whether this value is null or not.

class OrderByKeyEncoder {

public:
    OrderByKeyEncoder(vector<shared_ptr<ValueVector>>& orderByVectors, vector<bool>& isAscOrder,
        MemoryManager* memoryManager, uint16_t factorizedTableIdx);

    void encodeKeys();

    inline vector<shared_ptr<DataBlock>>& getKeyBlocks() { return keyBlocks; }

    inline uint64_t getNumBytesPerTuple() const { return numBytesPerTuple; }

    inline uint64_t getMaxNumTuplesPerBlock() const { return maxNumTuplesPerBlock; }

    inline uint64_t getNumTuplesInCurBlock() const { return keyBlocks.back()->numTuples; }

    static inline uint64_t getEncodedTupleIdxSizeInBytes() {
        return sizeof(nextLocalTupleIdx) - sizeof(factorizedTableIdx);
    }

    static uint64_t getEncodedTupleIdx(const uint8_t* tupleInfoBuffer);

    static uint64_t getEncodedFactorizedTableIdx(const uint8_t* tupleInfoBuffer);

    static uint64_t getEncodingSize(const DataType& dataType);

    static inline bool isNullVal(const uint8_t* nullBuffer, bool isAscOrder) {
        return *(nullBuffer) == (isAscOrder ? UINT8_MAX : 0);
    }

private:
    uint8_t flipSign(uint8_t key_byte);

    bool isLittleEndian();

    void encodeData(shared_ptr<ValueVector>& orderByVector, uint64_t idxInOrderByVector,
        uint8_t* keyBlockPtr, uint64_t keyColIdx);

    void encodeInt32(int32_t data, uint8_t* resultPtr);

    void encodeInt64(int64_t data, uint8_t* resultPtr);

    void encodeBool(bool data, uint8_t* resultPtr);

    void encodeDouble(double data, uint8_t* resultPtr);

    void encodeDate(date_t data, uint8_t* resultPtr);

    void encodeTimestamp(timestamp_t data, uint8_t* resultPtr);

    void encodeInterval(interval_t data, uint8_t* resultPtr);

    void encodeString(gf_string_t data, uint8_t* resultPtr);

    void encodeUnstr(uint8_t* resultPtr);

    void allocateMemoryIfFull();

private:
    MemoryManager* memoryManager;
    vector<shared_ptr<DataBlock>> keyBlocks;
    vector<shared_ptr<ValueVector>>& orderByVectors;
    vector<bool> isAscOrder;
    uint64_t numBytesPerTuple;
    uint64_t maxNumTuplesPerBlock;
    // We only use 6 bytes to represent the nextLocalTupleIdx, and 2 bytes to represent the
    // factorizedTableIdx. Only the lower 6 bytes of nextLocalTupleIdx are actually used, so the
    // MAX_LOCAL_TUPLE_IDX is 2^48-1.
    const uint64_t MAX_LOCAL_TUPLE_IDX = (1ull << 48) - 1;
    uint64_t nextLocalTupleIdx;
    uint16_t factorizedTableIdx;
};

} // namespace processor
} // namespace graphflow
