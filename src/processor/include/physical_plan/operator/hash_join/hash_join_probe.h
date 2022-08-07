#pragma once

#include "src/processor/include/physical_plan/operator/filtering_operator.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/result/result_set.h"

namespace graphflow {
namespace processor {

struct ProbeState {
    explicit ProbeState() : nextMatchedTupleIdx{0} {
        matchedTuples = make_unique<uint8_t*[]>(DEFAULT_VECTOR_CAPACITY);
        probedTuples = make_unique<uint8_t*[]>(DEFAULT_VECTOR_CAPACITY);
        probeSelVector = make_unique<SelectionVector>(DEFAULT_VECTOR_CAPACITY);
        probeSelVector->resetSelectorToValuePosBuffer();
        matchedSelVector = make_unique<SelectionVector>(DEFAULT_VECTOR_CAPACITY);
        matchedSelVector->resetSelectorToValuePosBuffer();
    }

    // Each key corresponds to a pointer with the same hash value from the ht directory.
    unique_ptr<uint8_t*[]> probedTuples;
    // Pointers to tuples in ht that actually matched.
    unique_ptr<uint8_t*[]> matchedTuples;
    // Selective index mapping each probed tuple to its probe side key vector.
    unique_ptr<SelectionVector> probeSelVector;
    unique_ptr<SelectionVector> matchedSelVector;
    sel_t nextMatchedTupleIdx;
};

struct ProbeDataInfo {

public:
    ProbeDataInfo(const DataPos& keyIDDataPos, vector<DataPos> nonKeyOutputDataPos)
        : keyIDDataPos{keyIDDataPos}, nonKeyOutputDataPos{std::move(nonKeyOutputDataPos)} {}

    ProbeDataInfo(const ProbeDataInfo& other)
        : ProbeDataInfo{other.keyIDDataPos, other.nonKeyOutputDataPos} {}

    inline uint32_t getKeyIDDataChunkPos() const { return keyIDDataPos.dataChunkPos; }

    inline uint32_t getKeyIDVectorPos() const { return keyIDDataPos.valueVectorPos; }

public:
    DataPos keyIDDataPos;
    vector<DataPos> nonKeyOutputDataPos;
};

// Probe side on left, i.e. children[0] and build side on right, i.e. children[1]
class HashJoinProbe : public PhysicalOperator, FilteringOperator {
public:
    HashJoinProbe(shared_ptr<HashJoinSharedState> sharedState,
        vector<uint64_t> flatDataChunkPositions, const ProbeDataInfo& probeDataInfo,
        bool isOutputAFlatTuple, unique_ptr<PhysicalOperator> probeChild,
        unique_ptr<PhysicalOperator> buildChild, uint32_t id, const string& paramsString)
        : PhysicalOperator{std::move(probeChild), std::move(buildChild), id, paramsString},
          FilteringOperator{1 /* numStatesToSave */}, sharedState{std::move(sharedState)},
          flatDataChunkPositions{std::move(flatDataChunkPositions)}, probeDataInfo{probeDataInfo},
          isOutputAFlatTuple{isOutputAFlatTuple} {}

    // This constructor is used for cloning only.
    HashJoinProbe(shared_ptr<HashJoinSharedState> sharedState,
        vector<uint64_t> flatDataChunkPositions, const ProbeDataInfo& probeDataInfo,
        bool isOutputAFlatTuple, unique_ptr<PhysicalOperator> probeChild, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{std::move(probeChild), id, paramsString},
          FilteringOperator{1 /* numStatesToSave */}, sharedState{std::move(sharedState)},
          flatDataChunkPositions{std::move(flatDataChunkPositions)}, probeDataInfo{probeDataInfo},
          isOutputAFlatTuple{isOutputAFlatTuple} {}

    PhysicalOperatorType getOperatorType() override { return HASH_JOIN_PROBE; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    // HashJoinProbe do not need to clone hashJoinBuild which is on a different pipeline.
    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<HashJoinProbe>(sharedState, flatDataChunkPositions, probeDataInfo,
            isOutputAFlatTuple, children[0]->clone(), id, paramsString);
    }

private:
    bool getNextBatchOfMatchedTuples();
    uint64_t populateResultSet();

private:
    shared_ptr<HashJoinSharedState> sharedState;
    vector<uint64_t> flatDataChunkPositions;

    ProbeDataInfo probeDataInfo;
    vector<shared_ptr<ValueVector>> vectorsToReadInto;
    vector<uint32_t> columnIdxsToReadFrom;
    shared_ptr<ValueVector> probeSideKeyVector;
    unique_ptr<ProbeState> probeState;
    bool isOutputAFlatTuple;
};
} // namespace processor
} // namespace graphflow
