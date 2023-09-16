#pragma once

#include "top_k.h"

namespace kuzu {
namespace processor {

struct TopKLocalScanState {
    std::vector<common::ValueVector*> vectorsToScan;
    std::unique_ptr<PayloadScanner> payloadScanner;

    void init(
        std::vector<DataPos>& outVectorPos, TopKSharedState& sharedState, ResultSet& resultSet);

    inline uint64_t scan() { return payloadScanner->scan(vectorsToScan); }
};

class TopKScan : public PhysicalOperator {
public:
    TopKScan(std::vector<DataPos> outVectorPos, std::shared_ptr<TopKSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::TOP_K_SCAN, std::move(child), id, paramsString},
          outVectorPos{std::move(outVectorPos)}, localState{std::make_unique<TopKLocalScanState>()},
          sharedState{std::move(sharedState)} {}

    inline bool isSource() const final { return true; }
    // Ordered table should be scanned in single-thread mode.
    inline bool canParallel() const override { return false; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<TopKScan>(
            outVectorPos, sharedState, children[0]->clone(), id, paramsString);
    }

private:
    std::vector<DataPos> outVectorPos;
    std::unique_ptr<TopKLocalScanState> localState;
    std::shared_ptr<TopKSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
