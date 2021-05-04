#pragma once

#include "src/common/include/types.h"
#include "src/common/include/vector/operations/vector_node_id_operations.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/result/result_set.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::common::operation;

namespace graphflow {
namespace processor {
struct ProbeState {
    explicit ProbeState(uint64_t pointersSize)
        : probeKeyPos(0), probedTuplesSize(0), matchedTuplesSize(0) {
        probedTuples = make_unique<uint8_t*[]>(pointersSize);
        matchedTuples = make_unique<uint8_t*[]>(pointersSize);
        probeSelVector = make_unique<uint16_t[]>(pointersSize);
    }
    // Each key corresponds to a pointer with the same hash value from the ht directory.
    unique_ptr<uint8_t*[]> probedTuples;
    // Pointers to tuples in ht that actually matched.
    unique_ptr<uint8_t*[]> matchedTuples;
    // Selective index mapping each matched tuple to its probe side key.
    unique_ptr<uint16_t[]> probeSelVector;
    uint64_t probeKeyPos;
    uint64_t probedTuplesSize;
    uint64_t matchedTuplesSize;
};

struct BuildSideVectorInfo {
    BuildSideVectorInfo(uint32_t numBytesPerValue, uint32_t outDataChunkIdx, uint32_t outVectorIdx,
        uint32_t vectorPtrsIdx)
        : numBytesPerValue(numBytesPerValue), outDataChunkPos(outDataChunkIdx),
          outVectorPos(outVectorIdx), vectorPtrsPos(vectorPtrsIdx) {}

    uint32_t numBytesPerValue;
    uint32_t outDataChunkPos;
    uint32_t outVectorPos;
    uint32_t vectorPtrsPos;
};

template<bool IS_OUT_DATACHUNK_FILTERED>
class HashJoinProbe : public PhysicalOperator {
public:
    HashJoinProbe(uint64_t buildSideKeyDataChunkPos, uint64_t buildSideKeyVectorPos,
        uint64_t probeSideKeyDataChunkPos, uint64_t probeSideKeyVectorPos,
        unique_ptr<PhysicalOperator> buildSidePrevOp, unique_ptr<PhysicalOperator> probeSidePrevOp);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        auto cloneOp = make_unique<HashJoinProbe>(buildSideKeyDataChunkPos, buildSideKeyVectorPos,
            probeSideKeyDataChunkPos, probeSideKeyVectorPos, buildSidePrevOp->clone(),
            prevOperator->clone());
        cloneOp->sharedState = this->sharedState;
        return cloneOp;
    }

public:
    unique_ptr<PhysicalOperator> buildSidePrevOp;
    shared_ptr<HashJoinSharedState> sharedState;

private:
    std::function<void(ValueVector&, ValueVector&)> vectorDecompressOp;
    std::function<void(ValueVector&, ValueVector&)> vectorHashOp;

    uint64_t buildSideKeyDataChunkPos;
    uint64_t buildSideKeyVectorPos;
    uint64_t probeSideKeyDataChunkPos;
    uint64_t probeSideKeyVectorPos;

    shared_ptr<DataChunk> buildSideKeyDataChunk;
    shared_ptr<ResultSet> buildSideNonKeyDataChunks;
    shared_ptr<NodeIDVector> probeSideKeyVector;
    shared_ptr<DataChunk> probeSideKeyDataChunk;
    shared_ptr<ResultSet> probeSideNonKeyDataChunks;

    vector<BuildSideVectorInfo> buildSideVectorInfos;
    shared_ptr<ValueVector> decompressedProbeKeyVector;
    shared_ptr<ValueVector> hashedProbeKeyVector;
    unique_ptr<ProbeState> probeState;
    shared_ptr<DataChunk> outKeyDataChunk;
    vector<unique_ptr<overflow_value_t[]>> buildSideVectorPtrs;

    // This function initializes:
    // 1) the outKeyDataChunk and appended unflat output data chunks, which are used to initialize
    // the output dataChunks. 2) buildSideVectorPtrs for build side unflat non-key data chunks.
    void initializeOutDataChunksAndVectorPtrs();

    // For each probe keyVector[i]=k, this function fills the probedTuples[i] with the pointer from
    // the slot that has hash(k) in directory (collision chain), without checking the actual key
    // value.
    void probeHTDirectory();
    // This function calls probeHTDirectory() to update probedTuples. And for each pointer in
    // probedTuples that points to a collision chain, this function finds all matched tuples along
    // the chain one batch at a time.
    void getNextBatchOfMatchedTuples();

    // This function reads matched tuples from ht and populates:
    // 1) outKeyDataChunk with values from probe side key data chunk, build side key data chunk
    // (except for build side keys), and also build side flat non-key data chunks. 2) populates
    // vector ptrs with ptrs to probe side unflat non-key data chunks.
    void populateOutKeyDataChunkAndVectorPtrs();
    // This function updates appended unflat output data chunks based on vector ptrs and
    // outKeyDataChunk.currPos.
    void updateAppendedUnFlatOutDataChunks();
};
} // namespace processor
} // namespace graphflow
