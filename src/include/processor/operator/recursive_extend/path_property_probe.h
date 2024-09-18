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

struct PathPropertyProbeInfo {
    DataPos pathPos;
    DataPos nodeIDsPos;

    std::vector<common::struct_field_idx_t> nodeFieldIndices;
    std::vector<common::struct_field_idx_t> relFieldIndices;
    std::vector<ft_col_idx_t> nodeTableColumnIndices;
    std::vector<ft_col_idx_t> relTableColumnIndices;

    PathPropertyProbeInfo() = default;
//    PathPropertyProbeInfo(const DataPos& pathPos,
//        std::vector<common::struct_field_idx_t> nodeFieldIndices,
//        std::vector<common::struct_field_idx_t> relFieldIndices,
//        std::vector<ft_col_idx_t> nodeTableColumnIndices,
//        std::vector<ft_col_idx_t> relTableColumnIndices)
//        : pathPos{pathPos}, nodeFieldIndices{std::move(nodeFieldIndices)},
//          relFieldIndices{std::move(relFieldIndices)},
//          nodeTableColumnIndices{std::move(nodeTableColumnIndices)},
//          relTableColumnIndices{std::move(relTableColumnIndices)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(PathPropertyProbeInfo);

private:
    PathPropertyProbeInfo(const PathPropertyProbeInfo& other)
        : pathPos{other.pathPos}, nodeIDsPos{other.nodeIDsPos}, nodeFieldIndices{other.nodeFieldIndices},
          relFieldIndices{other.relFieldIndices},
          nodeTableColumnIndices{other.nodeTableColumnIndices},
          relTableColumnIndices{other.relTableColumnIndices} {}
};

class PathPropertyProbe : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::PATH_PROPERTY_PROBE;

public:
    PathPropertyProbe(PathPropertyProbeInfo info,
        std::shared_ptr<PathPropertyProbeSharedState> sharedState,
        std::vector<std::unique_ptr<PhysicalOperator>> children, physical_op_id id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(children), id, std::move(printInfo)},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}
    PathPropertyProbe(PathPropertyProbeInfo info,
        std::shared_ptr<PathPropertyProbeSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> probeChild, physical_op_id id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(probeChild), id, std::move(printInfo)},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<PathPropertyProbe>(info.copy(), sharedState, children[0]->clone(),
            id, printInfo->copy());
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

    PathPropertyProbeInfo info;
    std::shared_ptr<PathPropertyProbeSharedState> sharedState;
    std::unique_ptr<PathPropertyProbeLocalState> localState;
    std::unique_ptr<Vectors> vectors;
};

} // namespace processor
} // namespace kuzu
