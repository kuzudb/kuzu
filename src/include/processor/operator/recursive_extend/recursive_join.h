#pragma once

#include <utility>

#include "bfs_scheduler.h"
#include "bfs_state.h"
#include "common/query_rel_type.h"
#include "frontier_scanner.h"
#include "planner/logical_plan/extend/recursive_join_type.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/result_collector.h"
#include "processor/operator/table_scan/factorized_table_scan.h"

namespace kuzu {
namespace processor {

class ScanFrontier;

struct RecursiveJoinSharedState {
    std::shared_ptr<MorselDispatcher> morselDispatcher;
    std::shared_ptr<FactorizedTableScanSharedState> inputFTableSharedState;
    std::vector<std::unique_ptr<NodeOffsetSemiMask>> semiMasks;

    RecursiveJoinSharedState(std::shared_ptr<MorselDispatcher> morselDispatcher,
        std::shared_ptr<FactorizedTableScanSharedState> inputFTableSharedState,
        std::vector<std::unique_ptr<NodeOffsetSemiMask>> semiMasks)
        : morselDispatcher{std::move(morselDispatcher)},
          inputFTableSharedState{std::move(inputFTableSharedState)}, semiMasks{
                                                                         std::move(semiMasks)} {}

    inline common::SchedulerType getSchedulerType() { return morselDispatcher->getSchedulerType(); }

    inline std::pair<GlobalSSSPState, SSSPLocalState> getBFSMorsel(
        const std::vector<common::ValueVector*>& vectorsToScan,
        const std::vector<ft_col_idx_t>& colIndicesToScan, common::ValueVector* srcNodeIDVector,
        BaseBFSMorsel* bfsMorsel, common::QueryRelType queryRelType) {
        return morselDispatcher->getBFSMorsel(inputFTableSharedState, vectorsToScan,
            colIndicesToScan, srcNodeIDVector, bfsMorsel, queryRelType);
    }

    inline int64_t writeDstNodeIDAndPathLength(
        const std::vector<common::ValueVector*>& vectorsToScan,
        const std::vector<ft_col_idx_t>& colIndicesToScan, common::ValueVector* dstNodeIDVector,
        common::ValueVector* pathLengthVector, common::table_id_t tableID,
        std::unique_ptr<BaseBFSMorsel>& bfsMorsel) {
        return morselDispatcher->writeDstNodeIDAndPathLength(inputFTableSharedState, vectorsToScan,
            colIndicesToScan, dstNodeIDVector, pathLengthVector, tableID, bfsMorsel);
    }
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

    RecursiveJoinDataInfo(const DataPos& srcNodePos, const DataPos& dstNodePos,
        std::unordered_set<common::table_id_t> dstNodeTableIDs, const DataPos& pathLengthPos,
        std::unique_ptr<ResultSetDescriptor> localResultSetDescriptor,
        const DataPos& recursiveDstNodeIDPos,
        std::unordered_set<common::table_id_t> recursiveDstNodeTableIDs,
        const DataPos& recursiveEdgeIDPos, const DataPos& pathPos)
        : srcNodePos{srcNodePos}, dstNodePos{dstNodePos},
          dstNodeTableIDs{std::move(dstNodeTableIDs)}, pathLengthPos{pathLengthPos},
          localResultSetDescriptor{std::move(localResultSetDescriptor)},
          recursiveDstNodeIDPos{recursiveDstNodeIDPos}, recursiveDstNodeTableIDs{std::move(
                                                            recursiveDstNodeTableIDs)},
          recursiveEdgeIDPos{recursiveEdgeIDPos}, pathPos{pathPos} {}

    inline std::unique_ptr<RecursiveJoinDataInfo> copy() {
        return std::make_unique<RecursiveJoinDataInfo>(srcNodePos, dstNodePos, dstNodeTableIDs,
            pathLengthPos, localResultSetDescriptor->copy(), recursiveDstNodeIDPos,
            recursiveDstNodeTableIDs, recursiveEdgeIDPos, pathPos);
    }
};

struct RecursiveJoinVectors {
    common::ValueVector* srcNodeIDVector = nullptr;
    common::ValueVector* dstNodeIDVector = nullptr;
    common::ValueVector* pathLengthVector = nullptr;
    common::ValueVector* pathVector = nullptr;              // STRUCT(LIST(NODE), LIST(REL))
    common::ValueVector* pathNodesVector = nullptr;         // LIST(NODE)
    common::ValueVector* pathNodesIDDataVector = nullptr;   // INTERNAL_ID
    common::ValueVector* pathRelsVector = nullptr;          // LIST(REL)
    common::ValueVector* pathRelsSrcIDDataVector = nullptr; // INTERNAL_ID
    common::ValueVector* pathRelsDstIDDataVector = nullptr; // INTERNAL_ID
    common::ValueVector* pathRelsIDDataVector = nullptr;    // INTERNAL_ID

    common::ValueVector* recursiveEdgeIDVector = nullptr;
    common::ValueVector* recursiveDstNodeIDVector = nullptr;
};

class RecursiveJoin : public PhysicalOperator {
public:
    RecursiveJoin(uint8_t lowerBound, uint8_t upperBound, common::QueryRelType queryRelType,
        planner::RecursiveJoinType joinType, std::shared_ptr<RecursiveJoinSharedState> sharedState,
        std::unique_ptr<RecursiveJoinDataInfo> dataInfo, std::vector<DataPos>& vectorsToScanPos,
        std::vector<ft_col_idx_t>& colIndicesToScan, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString,
        std::unique_ptr<PhysicalOperator> recursiveRoot)
        : PhysicalOperator{PhysicalOperatorType::RECURSIVE_JOIN, std::move(child), id,
              paramsString},
          lowerBound{lowerBound}, upperBound{upperBound}, queryRelType{queryRelType},
          joinType{joinType}, sharedState{std::move(sharedState)}, dataInfo{std::move(dataInfo)},
          vectorsToScanPos{vectorsToScanPos}, colIndicesToScan{colIndicesToScan},
          recursiveRoot{std::move(recursiveRoot)} {}

    inline RecursiveJoinSharedState* getSharedState() const { return sharedState.get(); }

    void initGlobalStateInternal(ExecutionContext* context) final;

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<RecursiveJoin>(lowerBound, upperBound, queryRelType, joinType,
            sharedState, dataInfo->copy(), vectorsToScanPos, colIndicesToScan, children[0]->clone(),
            id, paramsString, recursiveRoot->clone());
    }

private:
    void initLocalRecursivePlan(ExecutionContext* context);

    void populateTargetDstNodes();

    bool scanOutput();

    bool computeBFS(ExecutionContext* context);

    bool doBFSnThreadkMorsel(ExecutionContext* context);

#if defined(__GNUC__) || defined(__GNUG__)
    void computeBFSnThreadkMorsel(ExecutionContext* context);
#endif

    // Compute BFS for a given src node.
    void computeBFSOneThreadOneMorsel(ExecutionContext* context);

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
    std::unique_ptr<FrontiersScanner> frontiersScanner;
    std::unique_ptr<TargetDstNodes> targetDstNodes;

    /// NEW MEMBERS BEING ADDED
    std::vector<DataPos> vectorsToScanPos;
    std::vector<common::ValueVector*> vectorsToScan;
    std::vector<ft_col_idx_t> colIndicesToScan;
    std::unique_ptr<BaseBFSMorsel> bfsMorsel;
};

} // namespace processor
} // namespace kuzu
