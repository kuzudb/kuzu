#pragma once

#include "bfs_state.h"
#include "common/enums/query_rel_type.h"
#include "frontier_scanner.h"
#include "planner/operator/extend/recursive_join_type.h"
#include "processor/operator/mask.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class ScanFrontier;

struct RecursiveJoinSharedState {
    std::vector<std::unique_ptr<NodeOffsetSemiMask>> semiMasks;

    RecursiveJoinSharedState(std::vector<std::unique_ptr<NodeOffsetSemiMask>> semiMasks)
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
    DataPos recursiveDstNodeIDPos;
    std::unordered_set<common::table_id_t> recursiveDstNodeTableIDs;
    DataPos recursiveEdgeIDPos;
    // Path info
    DataPos pathPos;
    std::unordered_map<common::table_id_t, std::string> tableIDToName;

    RecursiveJoinDataInfo(const DataPos& srcNodePos, const DataPos& dstNodePos,
        std::unordered_set<common::table_id_t> dstNodeTableIDs, const DataPos& pathLengthPos,
        std::unique_ptr<ResultSetDescriptor> localResultSetDescriptor,
        const DataPos& recursiveDstNodeIDPos,
        std::unordered_set<common::table_id_t> recursiveDstNodeTableIDs,
        const DataPos& recursiveEdgeIDPos, const DataPos& pathPos,
        std::unordered_map<common::table_id_t, std::string> tableIDToName)
        : srcNodePos{srcNodePos}, dstNodePos{dstNodePos},
          dstNodeTableIDs{std::move(dstNodeTableIDs)}, pathLengthPos{pathLengthPos},
          localResultSetDescriptor{std::move(localResultSetDescriptor)},
          recursiveDstNodeIDPos{recursiveDstNodeIDPos}, recursiveDstNodeTableIDs{std::move(
                                                            recursiveDstNodeTableIDs)},
          recursiveEdgeIDPos{recursiveEdgeIDPos}, pathPos{pathPos}, tableIDToName{
                                                                        std::move(tableIDToName)} {}

    inline std::unique_ptr<RecursiveJoinDataInfo> copy() {
        return std::make_unique<RecursiveJoinDataInfo>(srcNodePos, dstNodePos, dstNodeTableIDs,
            pathLengthPos, localResultSetDescriptor->copy(), recursiveDstNodeIDPos,
            recursiveDstNodeTableIDs, recursiveEdgeIDPos, pathPos, tableIDToName);
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
    common::ValueVector* recursiveDstNodeIDVector = nullptr;
};

class RecursiveJoin : public PhysicalOperator {
public:
    RecursiveJoin(uint8_t lowerBound, uint8_t upperBound, common::QueryRelType queryRelType,
        planner::RecursiveJoinType joinType, std::shared_ptr<RecursiveJoinSharedState> sharedState,
        std::unique_ptr<RecursiveJoinDataInfo> dataInfo, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString,
        std::unique_ptr<PhysicalOperator> recursiveRoot)
        : PhysicalOperator{PhysicalOperatorType::RECURSIVE_JOIN, std::move(child), id,
              paramsString},
          lowerBound{lowerBound}, upperBound{upperBound}, queryRelType{queryRelType},
          joinType{joinType}, sharedState{std::move(sharedState)}, dataInfo{std::move(dataInfo)},
          recursiveRoot{std::move(recursiveRoot)} {}

    inline RecursiveJoinSharedState* getSharedState() const { return sharedState.get(); }

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<RecursiveJoin>(lowerBound, upperBound, queryRelType, joinType,
            sharedState, dataInfo->copy(), children[0]->clone(), id, paramsString,
            recursiveRoot->clone());
    }

private:
    void initLocalRecursivePlan(ExecutionContext* context);

    void populateTargetDstNodes();

    bool scanOutput();

    // Compute BFS for a given src node.
    void computeBFS(ExecutionContext* context);

    void updateVisitedNodes(common::nodeID_t boundNodeID);

private:
    uint8_t lowerBound;
    uint8_t upperBound;
    common::QueryRelType queryRelType;
    planner::RecursiveJoinType joinType;

    std::shared_ptr<RecursiveJoinSharedState> sharedState;
    std::unique_ptr<RecursiveJoinDataInfo> dataInfo;

    // Local recursive plan
    std::unique_ptr<ResultSet> localResultSet;
    std::unique_ptr<PhysicalOperator> recursiveRoot;
    ScanFrontier* scanFrontier;

    std::unique_ptr<RecursiveJoinVectors> vectors;
    std::unique_ptr<BaseBFSState> bfsState;
    std::unique_ptr<FrontiersScanner> frontiersScanner;
    std::unique_ptr<TargetDstNodes> targetDstNodes;
};

} // namespace processor
} // namespace kuzu
