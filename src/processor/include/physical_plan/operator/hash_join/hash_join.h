#pragma once

#include "src/common/include/operations/hash_operations.h"
#include "src/common/include/types.h"
#include "src/common/include/vector/operations/vector_node_id_operations.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_table.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/tuple/data_chunks.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::common::operation;

namespace graphflow {
namespace processor {

struct ProbeState {
    ProbeState(uint64_t pointersSize) : probeKeyPos(0), probedTuplesSize(0), matchedTuplesSize(0) {
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

// WARN: The hash join only supports INNER join type for now
// Key data structures:
// 1) outKeyDataChunk. Join results for vectors from the probe side key data chunk, the build
// side key data chunk (except for the build side key vector), and also build side flat non-key
// data chunks will be populated into the outKeyDataChunk.
// 2) buildSideVectorPtrs. Join results of vectors from build side unflat non-key data chunks
// will first be read from ht as vector ptrs, and kept in buildSideVectorPtrs. Each vector
// corresponds to an array of overflow_value_t structs.
// 3) appended unflat output data chunks. for each build side unflat non-key data chunk, append a
// new out data chunk in the output dataChunks.
// 4) output dataChunks. outKeyDataChunk + probe side non-key data chunks + appended unflat output
// data chunks.
template<bool IS_OUT_DATACHUNK_FILTERED>
class HashJoin : public PhysicalOperator {
public:
    HashJoin(uint64_t buildSideKeyDataChunkPos, uint64_t buildSideKeyVectorPos,
        uint64_t probeSideKeyDataChunkPos, uint64_t probeSideKeyVectorPos,
        unique_ptr<PhysicalOperator> buildSidePrevOp, unique_ptr<PhysicalOperator> probeSidePrevOp);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<HashJoin>(buildSideKeyDataChunkPos, buildSideKeyVectorPos,
            probeSideKeyDataChunkPos, probeSideKeyVectorPos, prevOperator->clone(),
            buildSidePrevOp->clone());
    }

private:
    std::function<void(ValueVector&, ValueVector&)> vectorDecompressOp;

    unique_ptr<MemoryManager> memManager;
    uint64_t buildSideKeyDataChunkPos;
    uint64_t buildSideKeyVectorPos;
    uint64_t probeSideKeyDataChunkPos;
    uint64_t probeSideKeyVectorPos;
    unique_ptr<PhysicalOperator> buildSidePrevOp;

    shared_ptr<DataChunk> buildSideKeyDataChunk;
    shared_ptr<DataChunks> buildSideNonKeyDataChunks;
    shared_ptr<NodeIDVector> probeSideKeyVector;
    shared_ptr<DataChunk> probeSideKeyDataChunk;
    shared_ptr<DataChunks> probeSideNonKeyDataChunks;

    vector<BuildSideVectorInfo> buildSideVectorInfos;
    unique_ptr<ValueVector> decompressedProbeKeyVector;
    unique_ptr<ProbeState> probeState;
    shared_ptr<DataChunk> outKeyDataChunk;
    vector<unique_ptr<overflow_value_t[]>> buildSideVectorPtrs;

    unique_ptr<HashTable> hashTable;
    bool isHTInitialized;

    void initializeHashTable();
    // This function initializes:
    // 1) the outKeyDataChunk and appended unflat output data chunks, which are used to initialize
    // the output dataChunks. 2) buildSideVectorPtrs for build side unflat non-key data chunks.
    void initializeOutDataChunksAndVectorPtrs();

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
