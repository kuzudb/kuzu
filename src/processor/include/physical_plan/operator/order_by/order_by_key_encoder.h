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
#include "src/processor/include/physical_plan/result/row_collection.h"

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
    explicit OrderByKeyEncoder(vector<shared_ptr<ValueVector>>& orderByVectors,
        vector<bool>& isAscOrder, MemoryManager& memoryManager, uint16_t threadID)
        : curBlockUsedEntries{0}, memoryManager{memoryManager}, orderByVectors{orderByVectors},
          isAscOrder{isAscOrder}, nextLocalRowID{0}, threadID{threadID} {
        keyBlocks.emplace_back(memoryManager.allocateBlock(SORT_BLOCK_SIZE));
        keyBlockEntrySizeInBytes = 0;
        for (auto& orderByVector : orderByVectors) {
            keyBlockEntrySizeInBytes += getEncodingSize(orderByVector->dataType);
        }
        // 6 bytes for rowID and 2 bytes for threadID
        keyBlockEntrySizeInBytes += 8;
        entriesPerBlock = SORT_BLOCK_SIZE / keyBlockEntrySizeInBytes;
        if (entriesPerBlock <= 0) {
            throw EncodingException(StringUtils::string_format(
                "EntrySize(%d bytes) is larger than the SORT_BLOCK_SIZE(%d bytes)",
                keyBlockEntrySizeInBytes, SORT_BLOCK_SIZE));
        }
    }

    void encodeKeys();

    inline vector<unique_ptr<MemoryBlock>>& getKeyBlocks() { return keyBlocks; }

    inline vector<bool>& getIsAscOrder() { return isAscOrder; }

    inline vector<shared_ptr<ValueVector>>& getOrderByVectors() { return orderByVectors; }

    inline uint64_t getKeyBlockEntrySizeInBytes() const { return keyBlockEntrySizeInBytes; }

    inline uint64_t getEntriesPerBlock() const { return entriesPerBlock; }

    inline uint64_t getCurBlockUsedEntries() const { return curBlockUsedEntries; }

    static inline uint64_t getEncodedRowIDSizeInBytes() {
        return sizeof(nextLocalRowID) - sizeof(threadID);
    }

    static inline uint64_t getEncodedRowID(const uint8_t* rowInfoBuffer) {
        uint64_t encodedRowID = 0;
        // Only the lower 6 bytes are used for localRowID.
        memcpy(&encodedRowID, rowInfoBuffer, getEncodedRowIDSizeInBytes());
        return encodedRowID;
    }

    static inline uint64_t getEncodedThreadID(const uint8_t* rowInfoBuffer) {
        uint16_t encodedThreadID = 0;
        // The lower 6 bytes are used for localRowID, only the higher two bytes are used for
        // threadID.
        memcpy(&encodedThreadID, rowInfoBuffer + getEncodedRowIDSizeInBytes(),
            sizeof(encodedThreadID));
        return encodedThreadID;
    }

    static uint64_t getEncodingSize(DataType dataType);

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

    void allocateMemoryIfFull();

private:
    MemoryManager& memoryManager;
    vector<unique_ptr<MemoryBlock>> keyBlocks;
    uint64_t curBlockUsedEntries;
    vector<shared_ptr<ValueVector>>& orderByVectors;
    vector<bool>& isAscOrder;
    uint64_t keyBlockEntrySizeInBytes;
    uint64_t entriesPerBlock;
    // We only use 6 bytes to represent the nextLocalRowID, and 2 bytes to represent the threadID.
    // Only the lower 6 bytes of nextLocalRowID are actually used, so the MAX_LOCAL_ROW_ID is
    // 2^48-1.
    const uint64_t MAX_LOCAL_ROW_ID = (1ull << 48) - 1;
    uint64_t nextLocalRowID;
    uint16_t threadID;
};

} // namespace processor
} // namespace graphflow
