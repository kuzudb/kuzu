#pragma once

#include "src/common/include/operations/hash_operations.h"
#include "src/common/include/types.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/result/result_set.h"
#include "src/processor/include/physical_plan/operator/sink/sink.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::common::operation;

namespace graphflow {
namespace processor {

// By default, overflow block size is 2MB
constexpr const int64_t DEFAULT_OVERFLOW_BLOCK_SIZE = 1 << 21;

struct BlockAppendInfo {
    BlockAppendInfo(uint8_t* buffer, uint64_t numEntries)
        : buffer{buffer}, numEntries{numEntries} {}

    uint8_t* buffer;
    uint64_t numEntries;
};

struct BlockHandle {
public:
    explicit BlockHandle(unique_ptr<MemoryBlock> block, uint64_t numEntries)
        : data{block->data}, freeSize{block->size}, numEntries{numEntries}, block{move(block)} {}

public:
    uint8_t* data;
    uint64_t freeSize;
    uint64_t numEntries;

private:
    unique_ptr<MemoryBlock> block;
};

// This is a shared state between HashJoinBuild and HashJoinProbe operators.
// Each clone of these two operators will share the same state.
// Inside the state, we keep the number of tuples inside the hash table (numEntries) a global
// list of references to ht blocks (htBlocks) and also overflow blocks (overflowBlocks), which
// are merged by each HashJoinBuild thread when they finished materializing thread-local tuples.
// Also, the state holds a global htDirectory, which will be updated by the last thread in the
// hash join build side task/pipeline, and probed by the HashJoinProbe operators.
class HashJoinSharedState {
public:
    explicit HashJoinSharedState()
        : htDirectory{nullptr}, numBytesForFixedTuplePart{0}, numEntries{0}, hashBitMask{0} {};

    mutex hashJoinSharedStateLock;

    unique_ptr<MemoryBlock> htDirectory;
    vector<BlockHandle> htBlocks;
    vector<BlockHandle> overflowBlocks;
    uint64_t numBytesForFixedTuplePart;
    uint64_t numEntries;
    uint64_t hashBitMask;
};

struct BuildDataChunksInfo {

public:
    BuildDataChunksInfo(const DataPos& keyDataPos, vector<bool> dataChunkPosToIsFlat)
        : keyDataPos{keyDataPos}, dataChunkPosToIsFlat{move(dataChunkPosToIsFlat)} {}

    BuildDataChunksInfo(const BuildDataChunksInfo& other)
        : BuildDataChunksInfo(other.keyDataPos, other.dataChunkPosToIsFlat) {}

public:
    DataPos keyDataPos;
    vector<bool> dataChunkPosToIsFlat;
};

class HashJoinBuild : public Sink {
public:
    HashJoinBuild(const BuildDataChunksInfo& buildDataChunksInfo, shared_ptr<ResultSet> resultSet,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id);

    void init() override;

    void execute() override;

    unique_ptr<PhysicalOperator> clone() override {
        auto clonedResultSet = make_shared<ResultSet>(resultSet->dataChunks.size());
        for (auto i = 0u; i < resultSet->dataChunks.size(); ++i) {
            clonedResultSet->insert(
                i, make_shared<DataChunk>(resultSet->dataChunks[i]->valueVectors.size()));
        }
        auto cloneOp = make_unique<HashJoinBuild>(
            dataChunksInfo, move(clonedResultSet), prevOperator->clone(), context, id);
        cloneOp->sharedState = this->sharedState;
        return cloneOp;
    }

    // Finalize the hash table directory
    void finalize() override;

    shared_ptr<HashJoinSharedState> sharedState;
    uint64_t numBytesForFixedTuplePart;

private:
    BuildDataChunksInfo dataChunksInfo;

    shared_ptr<DataChunk> keyDataChunk;
    vector<shared_ptr<DataChunk>> nonKeyDataChunks;
    uint64_t htBlockCapacity;
    uint64_t numEntries; // Thread-local num entries in htBlocks
    // Thread local main memory blocks holding |key|payload|prev| fields
    vector<BlockHandle> htBlocks;
    // Thread local overflow memory blocks for variable-sized values in tuples
    vector<BlockHandle> overflowBlocks;

    vector<BlockAppendInfo> allocateHTBlocks(uint64_t remaining);
    void appendPayloadVectorAsFixSizedValues(ValueVector& vector, uint8_t* appendBuffer,
        uint64_t valueOffsetInVector, uint64_t appendCount, bool isSingleValue) const;
    void appendPayloadVectorAsOverflowValue(
        ValueVector& vector, uint8_t* appendBuffer, uint64_t appendCount);
    void appendKeyVector(ValueVector& vector, uint8_t* appendBuffer, uint64_t valueOffsetInVector,
        uint64_t appendCount) const;
    overflow_value_t addVectorInOverflowBlocks(ValueVector& vector);
    void appendKeyDataChunk(
        const vector<BlockAppendInfo>& blockAppendInfos, uint64_t& tupleAppendOffset);
    void appendPayloadDataChunks(
        const vector<BlockAppendInfo>& blockAppendInfos, uint64_t& tupleAppendOffset);
    void appendResultSet();
};
} // namespace processor
} // namespace graphflow
