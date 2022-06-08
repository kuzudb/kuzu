#pragma once

#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/result/result_set.h"

using namespace std;

namespace graphflow {
namespace processor {

struct ProbeState {
    explicit ProbeState(uint64_t pointersSize)
        : probedTuple{nullptr}, numMatchedTuples{0}, probeSideKeyNodeID{
                                                         nodeID_t(UINT64_MAX, UINT64_MAX)} {
        matchedTuples = make_unique<uint8_t*[]>(pointersSize);
    }
    uint8_t* probedTuple;
    // Pointers to tuples in ht that actually matched.
    unique_ptr<uint8_t*[]> matchedTuples;
    uint64_t numMatchedTuples;
    nodeID_t probeSideKeyNodeID;
    uint64_t maxMorselSize;
};

struct ProbeDataInfo {

public:
    ProbeDataInfo(const DataPos& keyIDDataPos, vector<DataPos> nonKeyOutputDataPos)
        : keyIDDataPos{keyIDDataPos}, nonKeyOutputDataPos{move(nonKeyOutputDataPos)} {}

    ProbeDataInfo(const ProbeDataInfo& other)
        : ProbeDataInfo{other.keyIDDataPos, other.nonKeyOutputDataPos} {}

    inline uint32_t getKeyIDDataChunkPos() const { return keyIDDataPos.dataChunkPos; }

    inline uint32_t getKeyIDVectorPos() const { return keyIDDataPos.valueVectorPos; }

public:
    DataPos keyIDDataPos;
    vector<DataPos> nonKeyOutputDataPos;
};

// Probe side on left, i.e. children[0] and build side on right, i.e. children[1]
class HashJoinProbe : public PhysicalOperator {
public:
    HashJoinProbe(shared_ptr<HashJoinSharedState> sharedState, const ProbeDataInfo& probeDataInfo,
        unique_ptr<PhysicalOperator> probeChild, unique_ptr<PhysicalOperator> buildChild,
        uint32_t id, const string& paramsString)
        : PhysicalOperator{move(probeChild), move(buildChild), id, paramsString},
          sharedState{move(sharedState)}, probeDataInfo{probeDataInfo}, tuplePosToReadInProbedState{
                                                                            0} {}

    // This constructor is used for cloning only.
    HashJoinProbe(shared_ptr<HashJoinSharedState> sharedState, const ProbeDataInfo& probeDataInfo,
        unique_ptr<PhysicalOperator> probeChild, uint32_t id, const string& paramsString)
        : PhysicalOperator{move(probeChild), id, paramsString}, sharedState{move(sharedState)},
          probeDataInfo{probeDataInfo}, tuplePosToReadInProbedState{0} {}

    PhysicalOperatorType getOperatorType() override { return HASH_JOIN_PROBE; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    // HashJoinProbe do not need to clone hashJoinBuild which is on a different pipeline.
    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<HashJoinProbe>(
            sharedState, probeDataInfo, children[0]->clone(), id, paramsString);
    }

private:
    shared_ptr<HashJoinSharedState> sharedState;

    ProbeDataInfo probeDataInfo;
    uint64_t tuplePosToReadInProbedState;
    vector<shared_ptr<ValueVector>> vectorsToRead;
    vector<uint32_t> columnsToRead;
    shared_ptr<ValueVector> probeSideKeyVector;
    unique_ptr<ProbeState> probeState;

    void getNextBatchOfMatchedTuples();
    void populateResultSet();
};
} // namespace processor
} // namespace graphflow
