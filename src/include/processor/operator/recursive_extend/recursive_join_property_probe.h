#pragma once

#include "processor/operator/physical_operator.h"
#include "processor/operator/hash_join/hash_join_build.h"

namespace kuzu {
namespace processor {

struct RecursiveJoinPropertyProbeLocalState {
    std::unique_ptr<common::hash_t[]> hashes;
    std::unique_ptr<uint8_t*[]> probedTuples;
    std::unique_ptr<uint8_t*[]> matchedTuples;


    RecursiveJoinPropertyProbeLocalState() {
        hashes = std::make_unique<common::hash_t[]>(common::DEFAULT_VECTOR_CAPACITY);
        probedTuples = std::make_unique<uint8_t*[]>(common::DEFAULT_VECTOR_CAPACITY);
        matchedTuples = std::make_unique<uint8_t*[]>(common::DEFAULT_VECTOR_CAPACITY);
    }
};

class RecursiveJoinPropertyProbe : public PhysicalOperator {
public:
    RecursiveJoinPropertyProbe(std::shared_ptr<HashJoinSharedState> sharedState, std::unique_ptr<PhysicalOperator> probeChild,
        std::unique_ptr<PhysicalOperator> buildChild, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::RECURSIVE_JOIN_PROPERTY_PROBE,
              std::move(probeChild), std::move(buildChild), id, paramsString}, sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet *resultSet_, ExecutionContext *context) override;

    bool getNextTuplesInternal(ExecutionContext *context) override;

    std::unique_ptr<PhysicalOperator> clone() override;

private:
    std::shared_ptr<HashJoinSharedState> sharedState;
    DataPos pathPos;

    common::ValueVector* pathNodeIDVector;
    std::vector<common::ValueVector*> propertyVectors;

    std::unique_ptr<RecursiveJoinPropertyProbeLocalState> localState;
};

} // namespace processor
} // namespace kuzu