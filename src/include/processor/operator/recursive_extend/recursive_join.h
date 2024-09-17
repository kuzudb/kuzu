#pragma once

#include "bfs_state.h"
#include "common/enums/extend_direction.h"
#include "common/enums/query_rel_type.h"
#include "common/mask.h"
#include "frontier_scanner.h"
#include "planner/operator/extend/recursive_join_type.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class OffsetScanNodeTable;

struct RecursiveJoinSharedState {
    std::vector<std::unique_ptr<common::NodeOffsetLevelSemiMask>> semiMasks;

    explicit RecursiveJoinSharedState(
        std::vector<std::unique_ptr<common::NodeOffsetLevelSemiMask>> semiMasks)
        : semiMasks{std::move(semiMasks)} {}
};

struct RecursiveJoinDataInfo {
    // Join input info.
    DataPos srcNodePos;
    // Join output info.
    DataPos dstNodePos;
    std::unordered_set<common::table_id_t> dstNodeTableIDs;
    DataPos pathLengthPos;
    // Recursive join info.
    std::unique_ptr<ResultSetDescriptor> localResultSetDescriptor;
    DataPos recursiveSrcNodeIDPos;
    DataPos recursiveNodePredicateExecFlagPos;
    DataPos recursiveDstNodeIDPos;
    std::unordered_set<common::table_id_t> recursiveDstNodeTableIDs;
    DataPos recursiveEdgeIDPos;
    DataPos recursiveEdgeDirectionPos;
    // Path info
    DataPos pathPos;
    std::unordered_map<common::table_id_t, std::string> tableIDToName;

    RecursiveJoinDataInfo() = default;
    EXPLICIT_COPY_DEFAULT_MOVE(RecursiveJoinDataInfo);

private:
    RecursiveJoinDataInfo(const RecursiveJoinDataInfo& other) {
        srcNodePos = other.srcNodePos;
        dstNodePos = other.dstNodePos;
        dstNodeTableIDs = other.dstNodeTableIDs;
        pathLengthPos = other.pathLengthPos;
        localResultSetDescriptor = other.localResultSetDescriptor->copy();
        recursiveSrcNodeIDPos = other.recursiveSrcNodeIDPos;
        recursiveNodePredicateExecFlagPos = other.recursiveNodePredicateExecFlagPos;
        recursiveDstNodeIDPos = other.recursiveDstNodeIDPos;
        recursiveDstNodeTableIDs = other.recursiveDstNodeTableIDs;
        recursiveEdgeIDPos = other.recursiveEdgeIDPos;
        recursiveEdgeDirectionPos = other.recursiveEdgeDirectionPos;
        pathPos = other.pathPos;
        tableIDToName = other.tableIDToName;
    }
};

struct RecursiveJoinVectors {
    common::ValueVector* srcNodeIDVector = nullptr;
    common::ValueVector* dstNodeIDVector = nullptr;
    common::ValueVector* pathLengthVector = nullptr;
    common::ValueVector* pathVector = nullptr;               // STRUCT(LIST(NODE), LIST(REL))
    common::ValueVector* pathNodesVector = nullptr;          // LIST(NODE)
    common::ValueVector* pathNodesIDDataVector = nullptr;    // INTERNAL_ID
    common::ValueVector* pathNodesLabelDataVector = nullptr; // STRING
    common::ValueVector* pathRelsVector = nullptr;           // LIST(REL)
    common::ValueVector* pathRelsSrcIDDataVector = nullptr;  // INTERNAL_ID
    common::ValueVector* pathRelsDstIDDataVector = nullptr;  // INTERNAL_ID
    common::ValueVector* pathRelsIDDataVector = nullptr;     // INTERNAL_ID
    common::ValueVector* pathRelsLabelDataVector = nullptr;  // STRING

    common::ValueVector* recursiveEdgeIDVector = nullptr;
    common::ValueVector* recursiveEdgeDirectionVector = nullptr;
    common::ValueVector* recursiveDstNodeIDVector = nullptr;
    common::ValueVector* recursiveNodePredicateExecFlagVector = nullptr;
};

struct RecursiveJoinInfo {
    RecursiveJoinDataInfo dataInfo;
    uint8_t lowerBound = 0;
    uint8_t upperBound = UINT8_MAX;
    common::QueryRelType queryRelType{};
    planner::RecursiveJoinType joinType{};
    common::ExtendDirection direction{};
    bool extendFromSource = true;

    RecursiveJoinInfo() = default;
    EXPLICIT_COPY_DEFAULT_MOVE(RecursiveJoinInfo);

private:
    RecursiveJoinInfo(const RecursiveJoinInfo& other) {
        dataInfo = other.dataInfo.copy();
        lowerBound = other.lowerBound;
        upperBound = other.upperBound;
        queryRelType = other.queryRelType;
        joinType = other.joinType;
        direction = other.direction;
        extendFromSource = other.extendFromSource;
    }
};

class RecursiveJoin : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::RECURSIVE_JOIN;

public:
    RecursiveJoin(RecursiveJoinInfo info, std::shared_ptr<RecursiveJoinSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<PhysicalOperator> recursiveRoot, std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
          info{std::move(info)}, sharedState{std::move(sharedState)},
          recursiveRoot{std::move(recursiveRoot)}, recursiveSource{nullptr} {}

    std::vector<common::NodeSemiMask*> getSemiMask() const;

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<RecursiveJoin>(info.copy(), sharedState, children[0]->clone(), id,
            recursiveRoot->clone(), printInfo->copy());
    }

private:
    void initLocalRecursivePlan(ExecutionContext* context);

    void populateTargetDstNodes(ExecutionContext* context);

    bool scanOutput();

    // Compute BFS for a given src node.
    void computeBFS(ExecutionContext* context);

    void updateVisitedNodes(common::nodeID_t boundNodeID);

private:
    RecursiveJoinInfo info;
    std::shared_ptr<RecursiveJoinSharedState> sharedState;

    // Local recursive plan
    std::unique_ptr<ResultSet> localResultSet;
    std::unique_ptr<PhysicalOperator> recursiveRoot;
    OffsetScanNodeTable* recursiveSource;

    std::unique_ptr<RecursiveJoinVectors> vectors;
    std::unique_ptr<BaseBFSState> bfsState;
    std::unique_ptr<FrontiersScanner> frontiersScanner;
    std::unique_ptr<TargetDstNodes> targetDstNodes;
};

} // namespace processor
} // namespace kuzu
