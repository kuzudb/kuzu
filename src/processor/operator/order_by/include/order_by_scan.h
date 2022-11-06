#pragma once

#include "src/processor/operator/include/physical_operator.h"
#include "src/processor/operator/include/source_operator.h"
#include "src/processor/operator/order_by/include/order_by.h"

namespace graphflow {
namespace processor {

struct MergedKeyBlockScanState {
    bool scanSingleTuple;
    uint32_t nextTupleIdxToReadInMergedKeyBlock;
    shared_ptr<MergedKeyBlocks> mergedKeyBlock;
    uint32_t tupleIdxAndFactorizedTableIdxOffset;
    vector<uint32_t> colsToScan;
    unique_ptr<uint8_t*[]> tuplesToRead;
    unique_ptr<BlockPtrInfo> blockPtrInfo;
};

// To preserve the ordering of tuples, the orderByScan operator will only
// be executed in single-thread mode.
class OrderByScan : public PhysicalOperator, public SourceOperator {

public:
    OrderByScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        const vector<DataPos>& outDataPoses,
        shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, SourceOperator{move(
                                                               resultSetDescriptor)},
          outDataPoses{outDataPoses}, sharedState{move(sharedState)} {}

    // This constructor is used for cloning only.
    OrderByScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        const vector<DataPos>& outDataPoses,
        shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{id, paramsString}, SourceOperator{move(resultSetDescriptor)},
          outDataPoses{outDataPoses}, sharedState{move(sharedState)} {}

    PhysicalOperatorType getOperatorType() override { return ORDER_BY_SCAN; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<OrderByScan>(
            resultSetDescriptor->copy(), outDataPoses, sharedState, id, paramsString);
    }

    inline double getExecutionTime(Profiler& profiler) const override {
        return profiler.sumAllTimeMetricsWithKey(getTimeMetricKey());
    }

private:
    void initMergedKeyBlockScanStateIfNecessary();

private:
    vector<DataPos> outDataPoses;
    shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState;
    vector<shared_ptr<ValueVector>> vectorsToRead;
    unique_ptr<MergedKeyBlockScanState> mergedKeyBlockScanState;
};

} // namespace processor
} // namespace graphflow
