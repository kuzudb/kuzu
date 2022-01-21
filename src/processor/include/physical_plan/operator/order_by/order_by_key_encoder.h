#pragma once

#include <string>
#include <vector>

#include "src/common/include/configs.h"
#include "src/common/include/exception.h"
#include "src/common/include/gf_string.h"
#include "src/common/include/timestamp.h"
#include "src/common/include/types.h"
#include "src/common/include/utils.h"
#include "src/common/include/vector/value_vector.h"
#include "src/processor/include/physical_plan/result/factorized_table.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace processor {

struct KeyBlock {
    inline uint8_t* getMemBlockData() const { return memBlock->data; }

    explicit KeyBlock(unique_ptr<MemoryBlock> memBlock)
        : memBlock{move(memBlock)}, numEntriesInMemBlock{0} {}

    inline uint64_t getKeyBlockSize() { return memBlock->size; }

    unique_ptr<MemoryBlock> memBlock;
    uint64_t numEntriesInMemBlock;
};

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
    explicit OrderByKeyEncoder(vector<shared_ptr<ValueVector>>& orderByVectors,
        vector<bool>& isAscOrder, MemoryManager& memoryManager, uint16_t factorizedTableIdx)
        : memoryManager{memoryManager}, orderByVectors{orderByVectors}, isAscOrder{isAscOrder},
          nextLocalTupleIdx{0}, factorizedTableIdx{factorizedTableIdx} {
        keyBlocks.emplace_back(make_unique<KeyBlock>(memoryManager.allocateBlock(SORT_BLOCK_SIZE)));
        keyBlockEntrySizeInBytes = 0;
        for (auto& orderByVector : orderByVectors) {
            keyBlockEntrySizeInBytes += getEncodingSize(orderByVector->dataType);
        }
        // 6 bytes for tupleIdx and 2 bytes for factorizedTableIdx
        keyBlockEntrySizeInBytes += 8;
        maxEntriesPerBlock = SORT_BLOCK_SIZE / keyBlockEntrySizeInBytes;
        if (maxEntriesPerBlock <= 0) {
            throw EncodingException(StringUtils::string_format(
                "EntrySize(%d bytes) is larger than the SORT_BLOCK_SIZE(%d bytes)",
                keyBlockEntrySizeInBytes, SORT_BLOCK_SIZE));
        }
    }

    void encodeKeys();

    inline vector<shared_ptr<KeyBlock>>& getKeyBlocks() { return keyBlocks; }

    inline vector<bool>& getIsAscOrder() { return isAscOrder; }

    inline vector<shared_ptr<ValueVector>>& getOrderByVectors() { return orderByVectors; }

    inline uint64_t getKeyBlockEntrySizeInBytes() const { return keyBlockEntrySizeInBytes; }

    inline uint64_t getMaxEntriesPerBlock() const { return maxEntriesPerBlock; }

    inline uint64_t getCurBlockUsedEntries() const {
        return keyBlocks.back()->numEntriesInMemBlock;
    }

    static inline uint64_t getEncodedTupleIdxSizeInBytes() {
        return sizeof(nextLocalTupleIdx) - sizeof(factorizedTableIdx);
    }

    static inline uint64_t getEncodedTupleIdx(const uint8_t* tupleInfoBuffer) {
        uint64_t encodedTupleIdx = 0;
        // Only the lower 6 bytes are used for localTupleIdx.
        memcpy(&encodedTupleIdx, tupleInfoBuffer, getEncodedTupleIdxSizeInBytes());
        return encodedTupleIdx;
    }

    static inline uint64_t getEncodedFactorizedTableIdx(const uint8_t* tupleInfoBuffer) {
        uint16_t encodedFactorizedTableIdx = 0;
        // The lower 6 bytes are used for localTupleIdx, only the higher two bytes are used for
        // factorizedTableIdx.
        memcpy(&encodedFactorizedTableIdx, tupleInfoBuffer + getEncodedTupleIdxSizeInBytes(),
            sizeof(encodedFactorizedTableIdx));
        return encodedFactorizedTableIdx;
    }

    static uint64_t getEncodingSize(DataType dataType);

    static inline bool isNullVal(const uint8_t* nullBuffer, bool isAscOrder) {
        return *(nullBuffer) == (isAscOrder ? UINT8_MAX : 0);
    }

private:
    uint8_t flipSign(uint8_t key_byte);

    bool isLittleEndian();

    void encodeData(shared_ptr<ValueVector>& orderByVector, uint64_t idxInOrderByVector,
        uint8_t* keyBlockPtr, const uint64_t keyColIdx);

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
    MemoryManager& memoryManager;
    vector<shared_ptr<KeyBlock>> keyBlocks;
    vector<shared_ptr<ValueVector>>& orderByVectors;
    vector<bool> isAscOrder;
    uint64_t keyBlockEntrySizeInBytes;
    uint64_t maxEntriesPerBlock;
    // We only use 6 bytes to represent the nextLocalTupleIdx, and 2 bytes to represent the
    // factorizedTableIdx. Only the lower 6 bytes of nextLocalTupleIdx are actually used, so the
    // MAX_LOCAL_TUPLE_IDX is 2^48-1.
    const uint64_t MAX_LOCAL_TUPLE_IDX = (1ull << 48) - 1;
    uint64_t nextLocalTupleIdx;
    uint16_t factorizedTableIdx;
};

} // namespace processor
} // namespace graphflow
