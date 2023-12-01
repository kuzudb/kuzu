#pragma once

#include "common/enums/join_type.h"
#include "processor/operator/filtering_operator.h"
#include "processor/operator/hash_join/hash_join_build.h"
#include "processor/operator/physical_operator.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

struct ProbeState {
    explicit ProbeState() : nextMatchedTupleIdx{0} {
        matchedTuples = std::make_unique<uint8_t*[]>(common::DEFAULT_VECTOR_CAPACITY);
        probedTuples = std::make_unique<uint8_t*[]>(common::DEFAULT_VECTOR_CAPACITY);
        matchedSelVector =
            std::make_unique<common::SelectionVector>(common::DEFAULT_VECTOR_CAPACITY);
        matchedSelVector->resetSelectorToValuePosBuffer();
    }

    // Each key corresponds to a pointer with the same hash value from the ht directory.
    std::unique_ptr<uint8_t*[]> probedTuples;
    // Pointers to tuples in ht that actually matched.
    std::unique_ptr<uint8_t*[]> matchedTuples;
    // Selective index mapping each probed tuple to its probe side key vector.
    std::unique_ptr<common::SelectionVector> matchedSelVector;
    common::sel_t nextMatchedTupleIdx;
};

struct ProbeDataInfo {
public:
    ProbeDataInfo(std::vector<DataPos> keysDataPos, std::vector<DataPos> payloadsOutPos)
        : keysDataPos{std::move(keysDataPos)}, payloadsOutPos{std::move(payloadsOutPos)},
          markDataPos{UINT32_MAX, UINT32_MAX} {}

    ProbeDataInfo(const ProbeDataInfo& other)
        : ProbeDataInfo{other.keysDataPos, other.payloadsOutPos} {
        markDataPos = other.markDataPos;
    }

    inline uint32_t getNumPayloads() const { return payloadsOutPos.size(); }

public:
    std::vector<DataPos> keysDataPos;
    std::vector<DataPos> payloadsOutPos;
    DataPos markDataPos;
};

// Probe side on left, i.e. children[0] and build side on right, i.e. children[1]
class HashJoinProbe : public PhysicalOperator, public SelVectorOverWriter {
public:
    HashJoinProbe(std::shared_ptr<HashJoinSharedState> sharedState, common::JoinType joinType,
        bool flatProbe, const ProbeDataInfo& probeDataInfo,
        std::unique_ptr<PhysicalOperator> probeChild, std::unique_ptr<PhysicalOperator> buildChild,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::HASH_JOIN_PROBE, std::move(probeChild),
              std::move(buildChild), id, paramsString},
          sharedState{std::move(sharedState)}, joinType{joinType}, flatProbe{flatProbe},
          probeDataInfo{probeDataInfo} {}

    // This constructor is used for cloning only.
    HashJoinProbe(std::shared_ptr<HashJoinSharedState> sharedState, common::JoinType joinType,
        bool flatProbe, const ProbeDataInfo& probeDataInfo,
        std::unique_ptr<PhysicalOperator> probeChild, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::HASH_JOIN_PROBE, std::move(probeChild), id,
              paramsString},
          sharedState{std::move(sharedState)}, joinType{joinType}, flatProbe{flatProbe},
          probeDataInfo{probeDataInfo} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<HashJoinProbe>(sharedState, joinType, flatProbe, probeDataInfo,
            children[0]->clone(), id, paramsString);
    }

private:
    inline bool getMatchedTuples(ExecutionContext* context) {
        return flatProbe ? getMatchedTuplesForFlatKey(context) :
                           getMatchedTuplesForUnFlatKey(context);
    }
    bool getMatchedTuplesForFlatKey(ExecutionContext* context);
    // We can probe a batch of input tuples if we know they have at most one match.
    bool getMatchedTuplesForUnFlatKey(ExecutionContext* context);

    inline uint64_t getInnerJoinResult() {
        return flatProbe ? getInnerJoinResultForFlatKey() : getInnerJoinResultForUnFlatKey();
    }
    uint64_t getInnerJoinResultForFlatKey();
    uint64_t getInnerJoinResultForUnFlatKey();
    uint64_t getLeftJoinResult();
    uint64_t getMarkJoinResult();
    uint64_t getCountJoinResult();
    uint64_t getJoinResult();

private:
    std::shared_ptr<HashJoinSharedState> sharedState;
    common::JoinType joinType;
    bool flatProbe;

    ProbeDataInfo probeDataInfo;
    std::vector<common::ValueVector*> vectorsToReadInto;
    std::vector<uint32_t> columnIdxsToReadFrom;
    std::vector<common::ValueVector*> keyVectors;
    std::shared_ptr<common::ValueVector> markVector;
    std::unique_ptr<ProbeState> probeState;

    std::unique_ptr<common::ValueVector> hashVector;
    std::unique_ptr<common::ValueVector> tmpHashVector;
};

} // namespace processor
} // namespace kuzu
