#pragma once

#include "bfs_state.h"
#include "processor/operator/physical_operator.h"
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

struct BFSScanState {
    common::offset_t currentOffset;
    size_t numScanned;

    inline void resetState() {
        currentOffset = 0;
        numScanned = 0;
    }
};

class RecursiveJoin : public PhysicalOperator {
public:
    RecursiveJoin(uint8_t upperBound, storage::NodeTable* nodeTable,
        std::shared_ptr<FTableSharedState> inputFTableSharedState,
        std::vector<DataPos> vectorsToScanPos, std::vector<ft_col_idx_t> colIndicesToScan,
        const DataPos& srcNodeIDVectorPos, const DataPos& dstNodeIDVectorPos,
        const DataPos& distanceVectorPos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramsString, std::unique_ptr<PhysicalOperator> root)
        : PhysicalOperator{PhysicalOperatorType::SCAN_BFS_LEVEL, std::move(child), id,
              paramsString},
          upperBound{upperBound}, nodeTable{nodeTable}, inputFTableSharedState{std::move(
                                                            inputFTableSharedState)},
          vectorsToScanPos{std::move(vectorsToScanPos)}, colIndicesToScan{std::move(
                                                             colIndicesToScan)},
          srcNodeIDVectorPos{srcNodeIDVectorPos}, dstNodeIDVectorPos{dstNodeIDVectorPos},
          distanceVectorPos{distanceVectorPos}, root{std::move(root)}, bfsScanState{} {}

    static inline DataPos getTmpSrcNodeVectorPos() { return DataPos{0, 0}; }
    static inline DataPos getTmpDstNodeVectorPos() { return DataPos{1, 0}; }

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<RecursiveJoin>(upperBound, nodeTable, inputFTableSharedState,
            vectorsToScanPos, colIndicesToScan, srcNodeIDVectorPos, dstNodeIDVectorPos,
            distanceVectorPos, children[0]->clone(), id, paramsString, root->clone());
    }

private:
    static std::unique_ptr<ResultSet> getLocalResultSet();

    void initLocalRecursivePlan(ExecutionContext* context);
    // Compute BFS for a given src node.
    bool computeBFS(ExecutionContext* context);
    // Mark un-visited node as visited.
    void updateVisitedState();

    void scanDstNodes(size_t sizeToScan);

private:
    uint8_t upperBound;
    storage::NodeTable* nodeTable;
    std::shared_ptr<FTableSharedState> inputFTableSharedState;
    std::vector<DataPos> vectorsToScanPos;
    std::vector<ft_col_idx_t> colIndicesToScan;
    DataPos srcNodeIDVectorPos;
    DataPos dstNodeIDVectorPos;
    DataPos distanceVectorPos;

    // Local recursive plan
    std::unique_ptr<ResultSet> localResultSet;
    std::unique_ptr<PhysicalOperator> root;
    ScanFrontier* scanBFSLevel;

    std::unique_ptr<BFSMorsel> bfsMorsel;

    common::offset_t maxNodeOffset;
    std::vector<common::ValueVector*> vectorsToScan;
    std::shared_ptr<common::ValueVector> srcNodeIDVector;
    std::shared_ptr<common::ValueVector> dstNodeIDVector;
    std::shared_ptr<common::ValueVector> distanceVector;
    // vector for temporary recursive join result.
    std::shared_ptr<common::ValueVector> tmpDstNodeIDVector;

    BFSScanState bfsScanState;
};

} // namespace processor
} // namespace kuzu
