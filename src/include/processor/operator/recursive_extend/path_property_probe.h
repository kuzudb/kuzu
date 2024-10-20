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

// The input to PathPropertyProbe is a list of node ids and rel ids. We need to reconstruct src
//  & dst node ids from input node ids.
// e.g. given input src, [1, 2, 3], dst
// If all rels are in forward direction, i.e. ORDERED
// the src_ids should be [src, 1, 2, 3] and dst_ids should be [1, 2, 3, dst]
// If all rels are in backward direction, i.e. FLIP
// the src_ids should be [1, 2, 3, dst] and dst_ids should be [src, 1, 2, 3]
// If rels direction is not known at compile time, then we need to check the direction vector
// at runtime.
enum class PathSrcDstComputeInfo : uint8_t {
    ORDERED = 0,
    FLIP = 1,
    RUNTIME_CHECK = 2,
    UNKNOWN = 3,
};

struct PathPropertyProbeInfo {
    DataPos pathPos = DataPos();

    DataPos srcNodeIDPos = DataPos();
    DataPos dstNodeIDPos = DataPos();
    DataPos inputNodeIDsPos = DataPos();
    DataPos inputEdgeIDsPos = DataPos();
    DataPos directionPos = DataPos();

    PathSrcDstComputeInfo pathSrcDstComputeInfo = PathSrcDstComputeInfo::UNKNOWN;

    std::unordered_map<common::table_id_t, std::string> tableIDToName;

    std::vector<common::struct_field_idx_t> nodeFieldIndices;
    std::vector<common::struct_field_idx_t> relFieldIndices;
    std::vector<ft_col_idx_t> nodeTableColumnIndices;
    std::vector<ft_col_idx_t> relTableColumnIndices;

    bool extendFromSource = false;

    PathPropertyProbeInfo() = default;
    EXPLICIT_COPY_DEFAULT_MOVE(PathPropertyProbeInfo);

private:
    PathPropertyProbeInfo(const PathPropertyProbeInfo& other) {
        pathPos = other.pathPos;
        srcNodeIDPos = other.srcNodeIDPos;
        dstNodeIDPos = other.dstNodeIDPos;
        inputNodeIDsPos = other.inputNodeIDsPos;
        inputEdgeIDsPos = other.inputEdgeIDsPos;
        directionPos = other.directionPos;
        pathSrcDstComputeInfo = other.pathSrcDstComputeInfo;
        tableIDToName = other.tableIDToName;
        nodeFieldIndices = other.nodeFieldIndices;
        relFieldIndices = other.relFieldIndices;
        nodeTableColumnIndices = other.nodeTableColumnIndices;
        relTableColumnIndices = other.relTableColumnIndices;
        extendFromSource = other.extendFromSource;
    }
};

class PathPropertyProbe : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::PATH_PROPERTY_PROBE;

public:
    PathPropertyProbe(PathPropertyProbeInfo info,
        std::shared_ptr<PathPropertyProbeSharedState> sharedState,
        std::vector<std::unique_ptr<PhysicalOperator>> children, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(children), id, std::move(printInfo)},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}
    PathPropertyProbe(PathPropertyProbeInfo info,
        std::shared_ptr<PathPropertyProbeSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> probeChild, uint32_t id,
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
        const std::vector<ft_col_idx_t>& colIndicesToScan) const;

private:
    PathPropertyProbeInfo info;
    std::shared_ptr<PathPropertyProbeSharedState> sharedState;
    PathPropertyProbeLocalState localState;

    common::ValueVector* pathNodesVector = nullptr;
    common::ValueVector* pathRelsVector = nullptr;
    common::ValueVector* pathNodeIDsDataVector = nullptr;
    common::ValueVector* pathNodeLabelsDataVector = nullptr;
    common::ValueVector* pathRelIDsDataVector = nullptr;
    common::ValueVector* pathRelLabelsDataVector = nullptr;
    common::ValueVector* pathSrcNodeIDsDataVector = nullptr;
    common::ValueVector* pathDstNodeIDsDataVector = nullptr;

    std::vector<common::ValueVector*> pathNodesPropertyDataVectors;
    std::vector<common::ValueVector*> pathRelsPropertyDataVectors;

    common::ValueVector* inputSrcNodeIDVector = nullptr;
    common::ValueVector* inputDstNodeIDVector = nullptr;
    common::ValueVector* inputNodeIDsVector = nullptr;
    common::ValueVector* inputRelIDsVector = nullptr;
    common::ValueVector* inputDirectionVector = nullptr;
};

} // namespace processor
} // namespace kuzu
