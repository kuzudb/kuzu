#pragma once

#include "result_collector.h"

namespace kuzu {
namespace processor {

class CrossProduct : public PhysicalOperator {
public:
    CrossProduct(std::shared_ptr<FTableSharedState> sharedState, std::vector<DataPos> outVecPos,
        std::vector<uint32_t> colIndicesToScan, std::unique_ptr<PhysicalOperator> probeChild,
        std::unique_ptr<PhysicalOperator> buildChild, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::CROSS_PRODUCT, std::move(probeChild),
              std::move(buildChild), id, paramsString},
          sharedState{std::move(sharedState)}, outVecPos{std::move(outVecPos)},
          colIndicesToScan{std::move(colIndicesToScan)} {}

    // Clone only.
    CrossProduct(std::shared_ptr<FTableSharedState> sharedState, std::vector<DataPos> outVecPos,
        std::vector<uint32_t> colIndicesToScan, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::CROSS_PRODUCT, std::move(child), id, paramsString},
          sharedState{std::move(sharedState)}, outVecPos{std::move(outVecPos)},
          colIndicesToScan{std::move(colIndicesToScan)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CrossProduct>(
            sharedState, outVecPos, colIndicesToScan, children[0]->clone(), id, paramsString);
    }

private:
    std::shared_ptr<FTableSharedState> sharedState;
    std::vector<DataPos> outVecPos;
    std::vector<uint32_t> colIndicesToScan;

    uint64_t startIdx = 0u;
    std::vector<common::ValueVector*> vectorsToScan;
};

} // namespace processor
} // namespace kuzu
