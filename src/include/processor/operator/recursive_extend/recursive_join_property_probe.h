#pragma once

#include "processor/operator/hash_join/hash_join_build.h"
#include "processor/operator/physical_operator.h"

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
    RecursiveJoinPropertyProbe(std::shared_ptr<HashJoinSharedState> sharedState,
        const DataPos& pathPos, std::vector<ft_col_idx_t> colIndicesToScan,
        std::unique_ptr<PhysicalOperator> probeChild, std::unique_ptr<PhysicalOperator> buildChild,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::RECURSIVE_JOIN_PROPERTY_PROBE,
              std::move(probeChild), std::move(buildChild), id, paramsString},
          sharedState{std::move(sharedState)}, pathPos{pathPos}, colIndicesToScan{
                                                                     std::move(colIndicesToScan)} {}
    RecursiveJoinPropertyProbe(std::shared_ptr<HashJoinSharedState> sharedState,
        const DataPos& pathPos, std::vector<ft_col_idx_t> colIndicesToScan,
        std::unique_ptr<PhysicalOperator> probeChild, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::RECURSIVE_JOIN_PROPERTY_PROBE,
              std::move(probeChild), id, paramsString},
          sharedState{std::move(sharedState)}, pathPos{pathPos}, colIndicesToScan{
                                                                     std::move(colIndicesToScan)} {}

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<RecursiveJoinPropertyProbe>(
            sharedState, pathPos, colIndicesToScan, children[0]->clone(), id, paramsString);
    }

private:
    std::shared_ptr<HashJoinSharedState> sharedState;
    DataPos pathPos;
    std::vector<ft_col_idx_t> colIndicesToScan;

    common::ValueVector* pathNodesVector;
    common::ValueVector* pathNodesIDDataVector;
    std::vector<common::ValueVector*> pathNodesPropertyDataVectors;

    std::unique_ptr<RecursiveJoinPropertyProbeLocalState> localState;
};

} // namespace processor
} // namespace kuzu