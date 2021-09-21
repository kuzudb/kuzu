#pragma once

#include "src/common/include/types.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/result/result_set.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::common::operation;

namespace graphflow {
namespace processor {

struct ProbeState {
    explicit ProbeState(uint64_t pointersSize) : probedTuple{nullptr}, matchedTuplesSize{0} {
        matchedTuples = make_unique<uint8_t*[]>(pointersSize);
    }
    uint8_t* probedTuple;
    // Pointers to tuples in ht that actually matched.
    unique_ptr<uint8_t*[]> matchedTuples;
    uint64_t matchedTuplesSize;
    nodeID_t probeSideKeyNodeID;
};

struct BuildSideVectorInfo {
    BuildSideVectorInfo(uint32_t numBytesPerValue, uint32_t outDataChunkIdx, uint32_t outVectorIdx,
        uint32_t vectorPtrsIdx)
        : numBytesPerValue{numBytesPerValue}, resultDataChunkPos{outDataChunkIdx},
          resultVectorPos{outVectorIdx}, vectorPtrsPos{vectorPtrsIdx} {}

    uint32_t numBytesPerValue;
    uint32_t resultDataChunkPos;
    uint32_t resultVectorPos;
    uint32_t vectorPtrsPos;
};

struct ProbeDataChunksInfo {

public:
    ProbeDataChunksInfo(const DataPos& keyDataPos, uint32_t newFlatDataChunkPos,
        vector<uint32_t> newUnFlatDataChunksPos)
        : keyDataPos{keyDataPos}, newFlatDataChunkPos{newFlatDataChunkPos},
          newUnFlatDataChunksPos{move(newUnFlatDataChunksPos)} {}

    ProbeDataChunksInfo(const ProbeDataChunksInfo& other)
        : ProbeDataChunksInfo(
              other.keyDataPos, other.newFlatDataChunkPos, other.newUnFlatDataChunksPos){};

public:
    DataPos keyDataPos;
    uint32_t newFlatDataChunkPos;
    vector<uint32_t> newUnFlatDataChunksPos;
};

class HashJoinProbe : public PhysicalOperator {
public:
    HashJoinProbe(const BuildDataChunksInfo& buildDataChunksInfo,
        const ProbeDataChunksInfo& probeDataChunksInfo,
        vector<unordered_map<uint32_t, DataPos>> buildSideValueVectorsOutputPos,
        unique_ptr<PhysicalOperator> buildSidePrevOp, unique_ptr<PhysicalOperator> probeSidePrevOp,
        ExecutionContext& context, uint32_t id);

    void initResultSet(const shared_ptr<ResultSet>& resultSet) override;

    void reInitialize() override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        auto cloneOp = make_unique<HashJoinProbe>(buildDataChunksInfo, probeDataChunksInfo,
            buildSideValueVectorsOutputPos, buildSidePrevOp->clone(), prevOperator->clone(),
            context, id);
        cloneOp->sharedState = this->sharedState;
        return cloneOp;
    }

public:
    unique_ptr<PhysicalOperator> buildSidePrevOp;
    shared_ptr<HashJoinSharedState> sharedState;

private:
    BuildDataChunksInfo buildDataChunksInfo;
    ProbeDataChunksInfo probeDataChunksInfo;
    vector<unordered_map<uint32_t, DataPos>> buildSideValueVectorsOutputPos;

    uint64_t numProbeSidePrevKeyValueVectors;

    shared_ptr<ResultSet> buildSideResultSet;
    shared_ptr<ValueVector> probeSideKeyVector;
    shared_ptr<DataChunk> probeSideKeyDataChunk;
    shared_ptr<DataChunk> buildSideFlatResultDataChunk;

    vector<BuildSideVectorInfo> buildSideVectorInfos;
    unique_ptr<ProbeState> probeState;
    shared_ptr<DataChunk> resultKeyDataChunk;
    vector<unique_ptr<overflow_value_t[]>> buildSideVectorPtrs;

    void createVectorPtrs(DataChunk& buildSideDataChunk, uint32_t resultPos);
    void copyTuplesFromHT(DataChunk& resultDataChunk, uint64_t numResultVectors,
        uint64_t resultVectorStartPosition, uint64_t& tupleReadOffset,
        uint64_t startOffsetInResultVector, uint64_t numTuples);
    // This function performs essential initialization to the resultSet and buildSideVectorPtrs.
    void initializeResultSetAndVectorPtrs();
    // For each pointer in probedTuples that points to a collision chain, this function finds all
    // matched tuples along the chain one batch at a time.
    void getNextBatchOfMatchedTuples();
    // This function reads matched tuples from ht and populates:
    // 1) resultKeyDataChunk with values from build side key data chunk (except for build side
    // keys).
    // 2) build side flat non-key data chunks.
    // 3) populates vectorPtrs with pointers to vectors from build side unFlat non-key data chunks.
    void populateResultFlatDataChunksAndVectorPtrs();
    // This function updates appended unFlat output data chunks based on vectorPtrs and
    // buildSideFlatResultDataChunk.currIdx.
    void updateAppendedUnFlatDataChunks();
};
} // namespace processor
} // namespace graphflow
