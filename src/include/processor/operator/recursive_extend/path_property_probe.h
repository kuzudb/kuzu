#pragma once

#include "processor/operator/hash_join/hash_join_build.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct PathPropertyProbeSharedState {
    std::shared_ptr<HashJoinSharedState> nodeHashTableState;
    std::shared_ptr<HashJoinSharedState> relHashTableState;

    PathPropertyProbeSharedState(std::shared_ptr<HashJoinSharedState> nodeHashTableState,
        std::shared_ptr<HashJoinSharedState> relHashTableState)
        : nodeHashTableState{std::move(nodeHashTableState)},
          relHashTableState{std::move(relHashTableState)} {}
};

struct PathPropertyProbeLocalState {
    std::unique_ptr<common::hash_t[]> hashes;
    std::unique_ptr<uint8_t*[]> probedTuples;
    std::unique_ptr<uint8_t*[]> matchedTuples;

    PathPropertyProbeLocalState() {
        hashes = std::make_unique<common::hash_t[]>(common::DEFAULT_VECTOR_CAPACITY);
        probedTuples = std::make_unique<uint8_t*[]>(common::DEFAULT_VECTOR_CAPACITY);
        matchedTuples = std::make_unique<uint8_t*[]>(common::DEFAULT_VECTOR_CAPACITY);
    }
};

struct PathPropertyProbeDataInfo {
    DataPos pathPos;
    std::vector<common::struct_field_idx_t> nodeFieldIndices;
    std::vector<common::struct_field_idx_t> relFieldIndices;
    std::vector<ft_col_idx_t> nodeTableColumnIndices;
    std::vector<ft_col_idx_t> relTableColumnIndices;

    PathPropertyProbeDataInfo(const DataPos& pathPos,
        std::vector<common::struct_field_idx_t> nodeFieldIndices,
        std::vector<common::struct_field_idx_t> relFieldIndices,
        std::vector<ft_col_idx_t> nodeTableColumnIndices,
        std::vector<ft_col_idx_t> relTableColumnIndices)
        : pathPos{pathPos}, nodeFieldIndices{std::move(nodeFieldIndices)},
          relFieldIndices{std::move(relFieldIndices)},
          nodeTableColumnIndices{std::move(nodeTableColumnIndices)},
          relTableColumnIndices{std::move(relTableColumnIndices)} {}
    PathPropertyProbeDataInfo(const PathPropertyProbeDataInfo& other)
        : pathPos{other.pathPos}, nodeFieldIndices{other.nodeFieldIndices},
          relFieldIndices{other.relFieldIndices},
          nodeTableColumnIndices{other.nodeTableColumnIndices},
          relTableColumnIndices{other.relTableColumnIndices} {}

    std::unique_ptr<PathPropertyProbeDataInfo> copy() const {
        return std::make_unique<PathPropertyProbeDataInfo>(*this);
    }
};

class PathPropertyProbe : public PhysicalOperator {
public:
    PathPropertyProbe(std::unique_ptr<PathPropertyProbeDataInfo> info,
        std::shared_ptr<PathPropertyProbeSharedState> sharedState,
        std::vector<std::unique_ptr<PhysicalOperator>> children, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::PATH_PROPERTY_PROBE, std::move(children), id,
              paramsString},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}
    PathPropertyProbe(std::unique_ptr<PathPropertyProbeDataInfo> info,
        std::shared_ptr<PathPropertyProbeSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> probeChild, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::PATH_PROPERTY_PROBE, std::move(probeChild), id,
              paramsString},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<PathPropertyProbe>(info->copy(), sharedState, children[0]->clone(),
            id, paramsString);
    }

private:
    void probe(JoinHashTable* hashTable, uint64_t sizeProbed, uint64_t sizeToProbe,
        common::ValueVector* idVector, const std::vector<common::ValueVector*>& propertyVectors,
        const std::vector<ft_col_idx_t>& colIndicesToScan);

    struct Vectors {
        common::ValueVector* pathNodesVector;
        common::ValueVector* pathRelsVector;
        common::ValueVector* pathNodesIDDataVector;
        common::ValueVector* pathRelsIDDataVector;
        std::vector<common::ValueVector*> pathNodesPropertyDataVectors;
        std::vector<common::ValueVector*> pathRelsPropertyDataVectors;
    };

    std::unique_ptr<PathPropertyProbeDataInfo> info;
    std::shared_ptr<PathPropertyProbeSharedState> sharedState;
    std::unique_ptr<PathPropertyProbeLocalState> localState;
    std::unique_ptr<Vectors> vectors;
};

} // namespace processor
} // namespace kuzu
