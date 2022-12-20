#pragma once

#include "common/join_type.h"
#include "processor/operator/filtering_operator.h"
#include "processor/operator/hash_join/hash_join_build.h"
#include "processor/operator/physical_operator.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

struct ProbeState {
    explicit ProbeState() : nextMatchedTupleIdx{0} {
        matchedTuples = make_unique<uint8_t*[]>(DEFAULT_VECTOR_CAPACITY);
        probedTuples = make_unique<uint8_t*[]>(DEFAULT_VECTOR_CAPACITY);
        matchedSelVector = make_unique<SelectionVector>(DEFAULT_VECTOR_CAPACITY);
        matchedSelVector->resetSelectorToValuePosBuffer();
    }

    // Each key corresponds to a pointer with the same hash value from the ht directory.
    unique_ptr<uint8_t*[]> probedTuples;
    // Pointers to tuples in ht that actually matched.
    unique_ptr<uint8_t*[]> matchedTuples;
    // Selective index mapping each probed tuple to its probe side key vector.
    unique_ptr<SelectionVector> matchedSelVector;
    sel_t nextMatchedTupleIdx;
};

struct ProbeDataInfo {
public:
    ProbeDataInfo(vector<DataPos> keysDataPos, vector<DataPos> payloadsOutPos)
        : keysDataPos{std::move(keysDataPos)}, payloadsOutPos{std::move(payloadsOutPos)},
          markDataPos{UINT32_MAX, UINT32_MAX} {}

    ProbeDataInfo(const ProbeDataInfo& other)
        : ProbeDataInfo{other.keysDataPos, other.payloadsOutPos} {
        markDataPos = other.markDataPos;
    }

    inline uint32_t getNumPayloads() const { return payloadsOutPos.size(); }

public:
    vector<DataPos> keysDataPos;
    vector<DataPos> payloadsOutPos;
    DataPos markDataPos;
};

// Probe side on left, i.e. children[0] and build side on right, i.e. children[1]
class HashJoinProbe : public PhysicalOperator, SelVectorOverWriter {
public:
    HashJoinProbe(shared_ptr<HashJoinSharedState> sharedState, JoinType joinType,
        const ProbeDataInfo& probeDataInfo, unique_ptr<PhysicalOperator> probeChild,
        unique_ptr<PhysicalOperator> buildChild, uint32_t id, const string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::HASH_JOIN_PROBE, std::move(probeChild),
              std::move(buildChild), id, paramsString},
          sharedState{std::move(sharedState)}, joinType{joinType}, probeDataInfo{probeDataInfo} {}

    // This constructor is used for cloning only.
    // HashJoinProbe do not need to clone hashJoinBuild which is on a different pipeline.
    HashJoinProbe(shared_ptr<HashJoinSharedState> sharedState, JoinType joinType,
        const ProbeDataInfo& probeDataInfo, unique_ptr<PhysicalOperator> probeChild, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::HASH_JOIN_PROBE, std::move(probeChild), id,
              paramsString},
          sharedState{std::move(sharedState)}, joinType{joinType}, probeDataInfo{probeDataInfo} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<HashJoinProbe>(
            sharedState, joinType, probeDataInfo, children[0]->clone(), id, paramsString);
    }

private:
    bool hasMoreLeft();
    bool getNextBatchOfMatchedTuples();
    uint64_t getNextInnerJoinResult();
    uint64_t getNextLeftJoinResult();
    uint64_t getNextMarkJoinResult();
    void setVectorsToNull();

    uint64_t getNextJoinResult();

private:
    shared_ptr<HashJoinSharedState> sharedState;
    JoinType joinType;

    ProbeDataInfo probeDataInfo;
    vector<shared_ptr<ValueVector>> vectorsToReadInto;
    vector<uint32_t> columnIdxsToReadFrom;
    vector<shared_ptr<ValueVector>> keyVectors;
    shared_ptr<ValueVector> markVector;
    unique_ptr<ProbeState> probeState;
};

} // namespace processor
} // namespace kuzu
