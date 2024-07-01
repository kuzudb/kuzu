#pragma once

#include "top_k.h"

namespace kuzu {
namespace processor {

struct TopKLocalScanState {
    std::vector<common::ValueVector*> vectorsToScan;
    std::unique_ptr<PayloadScanner> payloadScanner;

    void init(std::vector<DataPos>& outVectorPos, TopKSharedState& sharedState,
        ResultSet& resultSet);

    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
    inline uint64_t scan() { return payloadScanner->scan(vectorsToScan); }
};

class TopKScan : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::TOP_K_SCAN;

public:
    TopKScan(std::vector<DataPos> outVectorPos, std::shared_ptr<TopKSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
          outVectorPos{std::move(outVectorPos)}, localState{std::make_unique<TopKLocalScanState>()},
          sharedState{std::move(sharedState)} {}

    inline bool isSource() const final { return true; }
    // Ordered table should be scanned in single-thread mode.
    inline bool isParallel() const override { return false; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<TopKScan>(outVectorPos, sharedState, children[0]->clone(), id,
            printInfo->copy());
    }

private:
    std::vector<DataPos> outVectorPos;
    std::unique_ptr<TopKLocalScanState> localState;
    std::shared_ptr<TopKSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
