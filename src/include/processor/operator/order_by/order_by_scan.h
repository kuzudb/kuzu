#pragma once

#include "processor/operator/order_by/order_by.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct MergedKeyBlockScanState {
    bool scanSingleTuple;
    uint32_t nextTupleIdxToReadInMergedKeyBlock;
    std::shared_ptr<MergedKeyBlocks> mergedKeyBlock;
    uint32_t tupleIdxAndFactorizedTableIdxOffset;
    std::vector<uint32_t> colsToScan;
    std::unique_ptr<uint8_t*[]> tuplesToRead;
    std::unique_ptr<BlockPtrInfo> blockPtrInfo;
};

// To preserve the ordering of tuples, the orderByScan operator will only
// be executed in single-thread mode.
class OrderByScan : public PhysicalOperator {
public:
    OrderByScan(std::vector<DataPos> outVectorPos,
        std::shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::ORDER_BY_SCAN, std::move(child), id, paramsString},
          outVectorPos{std::move(outVectorPos)}, sharedState{std::move(sharedState)} {}

    // This constructor is used for cloning only.
    OrderByScan(std::vector<DataPos> outVectorPos,
        std::shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::ORDER_BY_SCAN, id, paramsString},
          outVectorPos{std::move(outVectorPos)}, sharedState{std::move(sharedState)} {}

    inline bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<OrderByScan>(outVectorPos, sharedState, id, paramsString);
    }

private:
    void initMergedKeyBlockScanState();

private:
    std::vector<DataPos> outVectorPos;
    std::shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState;
    std::vector<common::ValueVector*> vectorsToRead;
    std::unique_ptr<MergedKeyBlockScanState> mergedKeyBlockScanState;
};

} // namespace processor
} // namespace kuzu
