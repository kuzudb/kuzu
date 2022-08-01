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
          currentNodeOffset{0}, hasMorselMask{false}, isInitialized{false} {
        maxNodeOffset = nodesMetadata->getMaxNodeOffset(READ_ONLY, labelID);
        morselMask.resize(
            (maxNodeOffset + DEFAULT_VECTOR_CAPACITY - 1) / DEFAULT_VECTOR_CAPACITY, false);
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

    inline void setMorselMask(uint64_t morselIdx) { morselMask[morselIdx] = true; }

    inline void setHashMorselMask() { hasMorselMask = true; }

private:
    mutex mtx;
    NodesMetadata* nodesMetadata;
    label_t labelID;
    uint64_t maxNodeOffset;
    uint64_t currentNodeOffset;
    bool hasMorselMask;
    bool isInitialized;
    vector<bool> morselMask;
};

class ScanNodeID : public PhysicalOperator, public SourceOperator {

public:
    ScanNodeID(unique_ptr<ResultSetDescriptor> resultSetDescriptor, NodeTable* nodeTable,
        const DataPos& outDataPos, shared_ptr<ScanNodeIDSharedState> sharedState, uint32_t id,
        string paramsString)
        : PhysicalOperator{id, move(paramsString)}, SourceOperator{move(resultSetDescriptor)},
          nodeTable{nodeTable}, outDataPos{outDataPos}, sharedState{move(sharedState)} {}

    PhysicalOperatorType getOperatorType() override { return SCAN_NODE_ID; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanNodeID>(
            resultSetDescriptor->copy(), nodeTable, outDataPos, sharedState, id, paramsString);
    }

    inline double getExecutionTime(Profiler& profiler) const override {
        return profiler.sumAllTimeMetricsWithKey(getTimeMetricKey());
    }
    inline ScanNodeIDSharedState* getSharedState() { return sharedState.get(); };

private:
    NodeTable* nodeTable;
    DataPos outDataPos;
    shared_ptr<ScanNodeIDSharedState> sharedState;

    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<ValueVector> outValueVector;
};

} // namespace processor
} // namespace graphflow
