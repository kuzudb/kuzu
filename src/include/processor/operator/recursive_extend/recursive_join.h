#pragma once

#include "bfs_state.h"
#include "path_scanner.h"
#include "planner/logical_plan/logical_operator/recursive_join_type.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/result_collector.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

class ScanFrontier : public PhysicalOperator {
public:
    ScanFrontier(DataPos nodeIDVectorPos, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SCAN_NODE_ID, id, paramsString},
          nodeIDVectorPos{nodeIDVectorPos} {}

    inline bool isSource() const override { return true; }

    inline void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) override {
        nodeIDVector = resultSet->getValueVector(nodeIDVectorPos);
    }

    bool getNextTuplesInternal(kuzu::processor::ExecutionContext* context) override;

    void setNodeID(common::nodeID_t nodeID) {
        nodeIDVector->setValue<common::nodeID_t>(0, nodeID);
        hasExecuted = false;
    }

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ScanFrontier>(nodeIDVectorPos, id, paramsString);
    }

private:
    DataPos nodeIDVectorPos;
    std::shared_ptr<common::ValueVector> nodeIDVector;
    bool hasExecuted;
};

struct RecursiveJoinSharedState {
    std::shared_ptr<FTableSharedState> inputFTableSharedState;
    std::unique_ptr<NodeOffsetSemiMask> semiMask;

    RecursiveJoinSharedState(
        std::shared_ptr<FTableSharedState> inputFTableSharedState, storage::NodeTable* nodeTable)
        : inputFTableSharedState{std::move(inputFTableSharedState)} {
        semiMask = std::make_unique<NodeOffsetSemiMask>(nodeTable);
    }
};

struct RecursiveJoinDataInfo {
    // Scanning input table info.
    std::vector<DataPos> vectorsToScanPos;
    std::vector<ft_col_idx_t> colIndicesToScan;
    // Join input info.
    DataPos srcNodePos;
    // Join output info.
    DataPos dstNodePos;
    DataPos pathLengthPos;
    // Recursive join info.
    DataPos tmpDstNodePos;
    // Path info
    planner::RecursiveJoinType joinType;
    DataPos pathPos;

    RecursiveJoinDataInfo(std::vector<DataPos> vectorsToScanPos,
        std::vector<ft_col_idx_t> colIndicesToScan, const DataPos& srcNodePos,
        const DataPos& dstNodePos, const DataPos& pathLengthPos, const DataPos& tmpDstNodePos,
        planner::RecursiveJoinType joinType)
        : RecursiveJoinDataInfo{std::move(vectorsToScanPos), std::move(colIndicesToScan),
              srcNodePos, dstNodePos, pathLengthPos, tmpDstNodePos, joinType, DataPos()} {}
    RecursiveJoinDataInfo(std::vector<DataPos> vectorsToScanPos,
        std::vector<ft_col_idx_t> colIndicesToScan, const DataPos& srcNodePos,
        const DataPos& dstNodePos, const DataPos& pathLengthPos, const DataPos& tmpDstNodePos,
        planner::RecursiveJoinType joinType, const DataPos& pathPos)
        : vectorsToScanPos{std::move(vectorsToScanPos)}, colIndicesToScan{std::move(
                                                             colIndicesToScan)},
          srcNodePos{srcNodePos}, dstNodePos{dstNodePos}, pathLengthPos{pathLengthPos},
          tmpDstNodePos{tmpDstNodePos}, joinType{joinType}, pathPos{pathPos} {}

    inline std::unique_ptr<RecursiveJoinDataInfo> copy() {
        return std::make_unique<RecursiveJoinDataInfo>(vectorsToScanPos, colIndicesToScan,
            srcNodePos, dstNodePos, pathLengthPos, tmpDstNodePos, joinType, pathPos);
    }
};

class BaseRecursiveJoin : public PhysicalOperator {
public:
    BaseRecursiveJoin(uint8_t lowerBound, uint8_t upperBound, storage::NodeTable* nodeTable,
        std::shared_ptr<RecursiveJoinSharedState> sharedState,
        std::unique_ptr<RecursiveJoinDataInfo> dataInfo, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString,
        std::unique_ptr<PhysicalOperator> recursiveRoot)
        : PhysicalOperator{PhysicalOperatorType::RECURSIVE_JOIN, std::move(child), id,
              paramsString},
          lowerBound{lowerBound}, upperBound{upperBound}, nodeTable{nodeTable},
          sharedState{std::move(sharedState)}, dataInfo{std::move(dataInfo)},
          recursiveRoot{std::move(recursiveRoot)} {}

    BaseRecursiveJoin(uint8_t lowerBound, uint8_t upperBound, storage::NodeTable* nodeTable,
        std::shared_ptr<RecursiveJoinSharedState> sharedState,
        std::unique_ptr<RecursiveJoinDataInfo> dataInfo, uint32_t id,
        const std::string& paramsString, std::unique_ptr<PhysicalOperator> recursiveRoot)
        : PhysicalOperator{PhysicalOperatorType::RECURSIVE_JOIN, id, paramsString},
          lowerBound{lowerBound}, upperBound{upperBound}, nodeTable{nodeTable},
          sharedState{std::move(sharedState)}, dataInfo{std::move(dataInfo)},
          recursiveRoot{std::move(recursiveRoot)} {}

    virtual ~BaseRecursiveJoin() = default;

    static inline DataPos getTmpSrcNodeVectorPos() { return DataPos{0, 0}; }

    inline NodeSemiMask* getSemiMask() const { return sharedState->semiMask.get(); }

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override = 0;

private:
    std::unique_ptr<ResultSet> getLocalResultSet();

    void initLocalRecursivePlan(ExecutionContext* context);

    bool scanOutput();

    // Compute BFS for a given src node.
    void computeBFS(ExecutionContext* context);

    void updateVisitedNodes(common::offset_t boundNodeOffset);

protected:
    uint8_t lowerBound;
    uint8_t upperBound;
    storage::NodeTable* nodeTable;
    std::shared_ptr<RecursiveJoinSharedState> sharedState;
    std::unique_ptr<RecursiveJoinDataInfo> dataInfo;

    // Local recursive plan
    std::unique_ptr<ResultSet> localResultSet;
    std::unique_ptr<PhysicalOperator> recursiveRoot;
    ScanFrontier* scanBFSLevel;

    // Vectors
    std::vector<common::ValueVector*> vectorsToScan;
    common::ValueVector* srcNodeIDVector;
    common::ValueVector* dstNodeIDVector;
    common::ValueVector* pathLengthVector;
    common::ValueVector* pathVector;

    common::ValueVector* tmpDstNodeIDVector; // temporary recursive join result.

    std::unique_ptr<BaseBFSMorsel> bfsMorsel;
    std::unique_ptr<FrontiersScanner> frontiersScanner;
};

} // namespace processor
} // namespace kuzu
