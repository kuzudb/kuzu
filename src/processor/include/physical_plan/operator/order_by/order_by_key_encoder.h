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
        vector<bool>& isAscOrder, MemoryManager& memoryManager)
        : curBlockUsedEntries{0}, memoryManager{memoryManager}, orderByVectors{orderByVectors},
          isAscOrder{isAscOrder}, entrySize{getEntrySize(orderByVectors)},
          entriesPerBlock{SORT_BLOCK_SIZE / entrySize}, nextGlobalRowID{0} {
        keyBlocks.emplace_back(memoryManager.allocateBlock(SORT_BLOCK_SIZE));
        if (entriesPerBlock <= 0) {
            throw EncodingException(StringUtils::string_format(
                "EntrySize(%d bytes) is larger than the SORT_BLOCK_SIZE(%d bytes)", entrySize,
                SORT_BLOCK_SIZE));
        }
    }

    void encodeKeys();

    inline vector<unique_ptr<MemoryBlock>>& getKeyBlocks() { return keyBlocks; }

    static uint64_t getEncodingSize(DataType dataType);

    static uint64_t getEntrySize(vector<shared_ptr<ValueVector>>& keys);

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

public:
    uint64_t curBlockUsedEntries;
    MemoryManager& memoryManager;

private:
    vector<unique_ptr<MemoryBlock>> keyBlocks;
    vector<shared_ptr<ValueVector>> orderByVectors;
    vector<bool> isAscOrder;
    uint64_t entrySize;
    uint64_t entriesPerBlock;
    uint64_t nextGlobalRowID;
};

} // namespace processor
} // namespace graphflow
