#pragma once

#include <mutex>

#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/source_operator.h"
#include "src/storage/store/include/node_table.h"

namespace graphflow {
namespace processor {

class ScanNodeIDSharedState {

public:
    explicit ScanNodeIDSharedState(NodeMetadata* nodeMetadata)
        : nodeMetadata{nodeMetadata}, maxNodeOffset{UINT64_MAX}, currentNodeOffset{0} {}

    inline unique_lock<mutex> acquireLock() { return unique_lock<mutex>{mtx}; }

    inline void initMaxNodeOffset(Transaction* transaction) {
        maxNodeOffset = nodeMetadata->getMaxNodeOffset(transaction);
    }

    pair<uint64_t, uint64_t> getNextRangeToRead();

private:
    mutex mtx;
    NodeMetadata* nodeMetadata;
    uint64_t maxNodeOffset;
    uint64_t currentNodeOffset;
};

class ScanNodeID : public PhysicalOperator, public SourceOperator {

public:
    ScanNodeID(unique_ptr<ResultSetDescriptor> resultSetDescriptor, NodeTable* nodeTable,
        const DataPos& outDataPos, shared_ptr<ScanNodeIDSharedState> sharedState, uint32_t id,
        string paramsString)
        : PhysicalOperator{id, paramsString}, SourceOperator{move(resultSetDescriptor)},
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

private:
    NodeTable* nodeTable;
    DataPos outDataPos;
    shared_ptr<ScanNodeIDSharedState> sharedState;

    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<ValueVector> outValueVector;
};

} // namespace processor
} // namespace graphflow
