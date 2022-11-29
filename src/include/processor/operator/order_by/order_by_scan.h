#pragma once

#include "processor/operator/order_by/order_by.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/source_operator.h"

namespace kuzu {
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
        vector<pair<DataPos, DataType>> outVectorPosAndTypes,
        shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{std::move(child), id, paramsString}, SourceOperator{std::move(
                                                                    resultSetDescriptor)},
          outVectorPosAndTypes{std::move(outVectorPosAndTypes)}, sharedState{
                                                                     std::move(sharedState)} {}

    // This constructor is used for cloning only.
    OrderByScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<pair<DataPos, DataType>> outVectorPosAndTypes,
        shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{id, paramsString}, SourceOperator{std::move(resultSetDescriptor)},
          outVectorPosAndTypes{std::move(outVectorPosAndTypes)}, sharedState{
                                                                     std::move(sharedState)} {}

    PhysicalOperatorType getOperatorType() override { return ORDER_BY_SCAN; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<OrderByScan>(
            resultSetDescriptor->copy(), outVectorPosAndTypes, sharedState, id, paramsString);
    }

    inline double getExecutionTime(Profiler& profiler) const override {
        return profiler.sumAllTimeMetricsWithKey(getTimeMetricKey());
    }

private:
    void initMergedKeyBlockScanState();

private:
    vector<pair<DataPos, DataType>> outVectorPosAndTypes;
    shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState;
    vector<shared_ptr<ValueVector>> vectorsToRead;
    unique_ptr<MergedKeyBlockScanState> mergedKeyBlockScanState;
};

} // namespace processor
} // namespace kuzu
