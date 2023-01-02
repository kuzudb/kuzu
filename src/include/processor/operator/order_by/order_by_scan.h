#pragma once

#include "processor/operator/order_by/order_by.h"
#include "processor/operator/physical_operator.h"

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
class OrderByScan : public PhysicalOperator {
public:
    OrderByScan(vector<DataPos> outVectorPos,
        shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::ORDER_BY_SCAN, std::move(child), id, paramsString},
          outVectorPos{std::move(outVectorPos)}, sharedState{std::move(sharedState)} {}

    // This constructor is used for cloning only.
    OrderByScan(vector<DataPos> outVectorPos,
        shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::ORDER_BY_SCAN, id, paramsString},
          outVectorPos{std::move(outVectorPos)}, sharedState{std::move(sharedState)} {}

    inline bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<OrderByScan>(outVectorPos, sharedState, id, paramsString);
    }

private:
    void initMergedKeyBlockScanState();

private:
    vector<DataPos> outVectorPos;
    shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState;
    vector<shared_ptr<ValueVector>> vectorsToRead;
    unique_ptr<MergedKeyBlockScanState> mergedKeyBlockScanState;
};

} // namespace processor
} // namespace kuzu
