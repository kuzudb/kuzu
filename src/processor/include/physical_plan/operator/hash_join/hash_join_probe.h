#pragma once

#include "src/common/include/types.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/result/result_set.h"

using namespace std;
using namespace graphflow::common;
using namespace graphflow::common::operation;

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
    uint64_t tuplePosToReadInProbedState;
    vector<DataPos> resultVectorsPos;
    vector<uint64_t> fieldsToRead;
    shared_ptr<ResultSet> buildSideResultSet;
    shared_ptr<ValueVector> probeSideKeyVector;
    unique_ptr<ProbeState> probeState;

    void constructResultVectorsPosAndFieldsToRead();
    void initializeResultSet();
    void getNextBatchOfMatchedTuples();
    void populateResultSet();
};
} // namespace processor
} // namespace graphflow
