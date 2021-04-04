#pragma once

#include "src/common/include/types.h"
#include "src/processor/include/memory_manager.h"
#include "src/processor/include/physical_plan/operator/hash_join/overflow_blocks.h"
#include "src/processor/include/physical_plan/operator/tuple/data_chunks.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace processor {

struct PayloadInfo {
    PayloadInfo(uint32_t numBytesPerValue, bool isStoredAsOverflow)
        : numBytesPerValue(numBytesPerValue), isStoredAsOverflow(isStoredAsOverflow) {}

    uint32_t numBytesPerValue;
    bool isStoredAsOverflow;
};

struct BlockAppendInfo {
    BlockAppendInfo(uint8_t* buffer, uint64_t numEntries)
        : buffer(buffer), numEntries(numEntries) {}

    uint8_t* buffer;
    uint64_t numEntries;
};

constexpr const uint64_t HT_BLOCK_SIZE = 262144; // By default, block size is 256KB

//! The HashTable data structure for HashJoin. for now, we assume hash only happens on nodeId.
//! Layout of tuple in blocks: |key|payload|prev|.
//! Inside payload, variable-sized values are stored in overflowBlocks.
class HashTable {
public:
    HashTable(MemoryManager* memManager, vector<PayloadInfo>& payloadInfos);

    void addDataChunks(
        DataChunk& keyDataChunk, uint64_t keyVectorPos, DataChunks& nonKeyDataChunks);
    void buildDirectory();

    uint64_t numBytesForFixedTuplePart;
    uint64_t hashBitMask;
    unique_ptr<BlockHandle> htDirectory;

    inline void setMemoryManager(MemoryManager* memManager) {
        this->memManager = memManager;
        overflowBlocks.memManager = memManager;
    }

private:
    mutex htLock;
    MemoryManager* memManager;

    atomic_uint64_t numEntries;
    uint64_t htBlockCapacity;

    // The main memory blocks holding |key|payload|prev| fields
    vector<unique_ptr<BlockHandle>> htBlocks;
    // The overflow memory blocks for variable-sized values in tuples
    OverflowBlocks overflowBlocks;

    void allocateHTBlocks(uint64_t remaining, vector<BlockAppendInfo>& tuplesAppendInfos);

    inline void appendPayloadVectorAsFixSizedValues(ValueVector& vector, uint8_t* appendBuffer,
        uint64_t valueOffsetInVector, uint64_t appendCount, bool isSingleValue) const;
    inline void appendPayloadVectorAsOverflowValue(
        ValueVector& vector, uint8_t* appendBuffer, uint64_t appendCount);

    void appendKeyVector(NodeIDVector& vector, uint8_t* appendBuffer, uint64_t valueOffsetInVector,
        uint64_t appendCount) const;

    HashTable(const HashTable&) = delete; // We don't allow copy of ht
};

} // namespace processor
} // namespace graphflow
