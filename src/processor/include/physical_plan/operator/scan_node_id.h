#pragma once

#include <mutex>

#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/source_operator.h"
#include "src/storage/store/include/node_table.h"

namespace graphflow {
namespace processor {

class ScanNodeIDSharedState {

public:
    ScanNodeIDSharedState(NodesMetadata* nodesMetadata, label_t labelID)
        : nodesMetadata{nodesMetadata}, labelID{labelID}, maxNodeOffset{UINT64_MAX},
          currentNodeOffset{0}, hasSemiMask{false}, isInitialized{false} {
        maxNodeOffset = nodesMetadata->getMaxNodeOffset(READ_ONLY, labelID);
    }

    // TODO(Guodong): move transaction to mapper and constructor.
    inline void initMaxNodeOffset(Transaction* transaction) {
        unique_lock uLck{mtx};
        if (!isInitialized) {
            isInitialized = true;
            maxNodeOffset = nodesMetadata->getMaxNodeOffset(transaction, labelID);
        }
    }

    pair<uint64_t, uint64_t> getNextRangeToRead();

    inline void setMaskForNode(uint64_t nodeOffset) {
        nodeMask[nodeOffset] = true;
        morselMask[nodeOffset >> DEFAULT_VECTOR_CAPACITY_LOG_2] = true;
    }
    inline bool getMaskForNode(uint64_t nodeOffset) { return nodeMask[nodeOffset]; }

    inline void setHasSemiMask() {
        hasSemiMask = true;
        morselMask.resize(
            (maxNodeOffset + DEFAULT_VECTOR_CAPACITY - 1) / DEFAULT_VECTOR_CAPACITY, false);
        nodeMask.resize(maxNodeOffset, false);
    }
    inline bool getHasSemiMask() const { return hasSemiMask; }

private:
    mutex mtx;
    NodesMetadata* nodesMetadata;
    label_t labelID;
    uint64_t maxNodeOffset;
    uint64_t currentNodeOffset;
    bool hasSemiMask;
    bool isInitialized;
    vector<bool> morselMask;
    vector<bool> nodeMask;
};

class ScanNodeID : public PhysicalOperator, public SourceOperator {

public:
    ScanNodeID(unique_ptr<ResultSetDescriptor> resultSetDescriptor, string nodeID,
        NodeTable* nodeTable, const DataPos& outDataPos,
        shared_ptr<ScanNodeIDSharedState> sharedState, uint32_t id, string paramsString)
        : PhysicalOperator{id, move(paramsString)}, SourceOperator{move(resultSetDescriptor)},
          nodeID{move(nodeID)}, nodeTable{nodeTable}, outDataPos{outDataPos}, sharedState{move(
                                                                                  sharedState)} {}

    PhysicalOperatorType getOperatorType() override { return SCAN_NODE_ID; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    inline string getNodeID() const { return nodeID; }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanNodeID>(resultSetDescriptor->copy(), nodeID, nodeTable, outDataPos,
            sharedState, id, paramsString);
    }

    inline double getExecutionTime(Profiler& profiler) const override {
        return profiler.sumAllTimeMetricsWithKey(getTimeMetricKey());
    }
    inline ScanNodeIDSharedState* getSharedState() { return sharedState.get(); };

private:
    void setSelectedPositions(
        node_offset_t startOffset, node_offset_t endOffset, SelectionVector& selVector);

private:
    string nodeID;
    NodeTable* nodeTable;
    DataPos outDataPos;
    shared_ptr<ScanNodeIDSharedState> sharedState;

    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<ValueVector> outValueVector;
};

} // namespace processor
} // namespace graphflow
