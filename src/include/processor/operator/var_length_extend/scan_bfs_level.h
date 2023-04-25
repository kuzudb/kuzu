#pragma once

#include "bfs_state.h"
#include "processor/operator/physical_operator.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

class DummyScan : public PhysicalOperator {
public:
    DummyScan(DataPos dataPos, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SCAN_NODE_ID, id, paramsString}, dataPos{dataPos} {
    }

    bool isSource() const override  {
        return true;
    }

    void initLocalStateInternal(kuzu::processor::ResultSet* resultSet_,
        kuzu::processor::ExecutionContext* context) override {
        nodeIDVector = resultSet->getValueVector(dataPos);
    }

    bool getNextTuplesInternal(kuzu::processor::ExecutionContext* context) override {
        if (!hasExecuted) {
            hasExecuted = true;
            return true;
        }
        return false;
    }

    void setNodeID(common::nodeID_t nodeID) {
        nodeIDVector->setValue<common::nodeID_t>(0, nodeID);
        hasExecuted = false;
    }

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<DummyScan>(dataPos, id, paramsString);
    }

private:
    DataPos dataPos;
    std::shared_ptr<common::ValueVector> nodeIDVector;
    bool hasExecuted;
};

class ScanBFSLevel : public PhysicalOperator {
public:
    ScanBFSLevel(uint8_t upperBound, storage::NodeTable* nodeTable,
        const DataPos& srcNodeIDVectorPos, const DataPos& dstNodeIDVectorPos,
        const DataPos& distanceVectorPos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramsString, std::unique_ptr<PhysicalOperator> root)
        : PhysicalOperator{PhysicalOperatorType::SCAN_BFS_LEVEL, std::move(child), id,
              paramsString},
          upperBound{upperBound}, nodeTable{nodeTable}, srcNodeIDVectorPos{srcNodeIDVectorPos},
          dstNodeIDVectorPos{dstNodeIDVectorPos}, distanceVectorPos{distanceVectorPos}, root{std::move(root)} {}

    inline void setThreadLocalSharedState(std::shared_ptr<SSPThreadLocalSharedState> state) {
        threadLocalSharedState = std::move(state);
    }

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ScanBFSLevel>(upperBound, nodeTable, srcNodeIDVectorPos,
            dstNodeIDVectorPos, distanceVectorPos, children[0]->clone(), id, paramsString,
            root->clone());
    }

private:
    bool computeBFS(ExecutionContext* context);
    void updateVisitedState() {
        auto visitedNodes = threadLocalSharedState->sspMorsel->visitedNodes;
        DataPos dataPos{1, 0};
        auto nodeIDVector = localResultSet->getValueVector(dataPos);
        for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            auto nodeID = nodeIDVector->getValue<common::nodeID_t>(pos);
            if (visitedNodes[nodeID.offset] == VisitedState::NOT_VISITED) {
                threadLocalSharedState->sspMorsel->markOnVisit(nodeID.offset);
                continue;
            }
        }
    }

private:
    uint8_t upperBound;
    storage::NodeTable* nodeTable;
    DataPos srcNodeIDVectorPos;
    DataPos dstNodeIDVectorPos;
    DataPos distanceVectorPos;
    std::shared_ptr<SSPThreadLocalSharedState> threadLocalSharedState;

    std::unique_ptr<ResultSet> localResultSet;
    std::unique_ptr<PhysicalOperator> root;
    DummyScan* dummyScan;

    common::offset_t maxNodeOffset;
    std::shared_ptr<common::ValueVector> srcNodeIDVector;
    std::shared_ptr<common::ValueVector> dstNodeIDVector;
    std::shared_ptr<common::ValueVector> distanceVector;

    common::offset_t currentOffset;
    size_t sizeScanned;
};

} // namespace processor
} // namespace kuzu