#pragma once

#include "processor/operator/order_by/sort_state.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct OrderByScanLocalState {
    std::vector<common::ValueVector*> vectorsToRead;
    std::unique_ptr<PayloadScanner> payloadScanner;
    uint64_t numTuples;
    uint64_t numTuplesRead;

    void init(std::vector<DataPos>& outVectorPos, SortSharedState& sharedState,
        ResultSet& resultSet);

    // NOLINTNEXTLINE(readability-make-member-function-const): Updates vectorsToRead.
    uint64_t scan() {
        uint64_t tuplesRead = payloadScanner->scan(vectorsToRead);
        numTuplesRead += tuplesRead;
        return tuplesRead;
    }
};

// To preserve the ordering of tuples, the orderByScan operator will only
// be executed in single-thread mode.
class OrderByScan : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::ORDER_BY_SCAN;

public:
    OrderByScan(std::vector<DataPos> outVectorPos, std::shared_ptr<SortSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
          outVectorPos{std::move(outVectorPos)},
          localState{std::make_unique<OrderByScanLocalState>()},
          sharedState{std::move(sharedState)} {}

    // This constructor is used for cloning only.
    OrderByScan(std::vector<DataPos> outVectorPos, std::shared_ptr<SortSharedState> sharedState,
        uint32_t id, std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, id, std::move(printInfo)}, outVectorPos{std::move(outVectorPos)},
          localState{std::make_unique<OrderByScanLocalState>()},
          sharedState{std::move(sharedState)} {}

    bool isSource() const final { return true; }
    // Ordered table should be scanned in single-thread mode.
    bool isParallel() const final { return false; }

    bool getNextTuplesInternal(ExecutionContext* context) final;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<OrderByScan>(outVectorPos, sharedState, id, printInfo->copy());
    }

    double getProgress(ExecutionContext* context) const override;

private:
    std::vector<DataPos> outVectorPos;
    std::unique_ptr<OrderByScanLocalState> localState;
    std::shared_ptr<SortSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
