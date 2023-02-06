#pragma once

#include "processor/operator/table_scan/base_table_scan.h"

namespace kuzu {
namespace processor {

class UnionAllScanSharedState {

public:
    explicit UnionAllScanSharedState(
        std::vector<std::shared_ptr<FTableSharedState>> fTableSharedStates)
        : fTableSharedStates{std::move(fTableSharedStates)}, fTableToScanIdx{0} {}

    uint64_t getMaxMorselSize() const;
    std::unique_ptr<FTableScanMorsel> getMorsel(uint64_t maxMorselSize);

private:
    std::mutex mtx;
    std::vector<std::shared_ptr<FTableSharedState>> fTableSharedStates;
    uint64_t fTableToScanIdx;
};

class UnionAllScan : public BaseTableScan {
public:
    UnionAllScan(std::vector<DataPos> outVecPositions, std::vector<uint32_t> colIndicesToScan,
        std::shared_ptr<UnionAllScanSharedState> sharedState,
        std::vector<std::unique_ptr<PhysicalOperator>> children, uint32_t id,
        const std::string& paramsString)
        : BaseTableScan{PhysicalOperatorType::UNION_ALL_SCAN, std::move(outVecPositions),
              std::move(colIndicesToScan), std::move(children), id, paramsString},
          sharedState{std::move(sharedState)} {}

    // For clone only
    UnionAllScan(std::vector<DataPos> outVecPositions, std::vector<uint32_t> colIndicesToScan,
        std::shared_ptr<UnionAllScanSharedState> sharedState, uint32_t id,
        const std::string& paramsString)
        : BaseTableScan{PhysicalOperatorType::UNION_ALL_SCAN, std::move(outVecPositions),
              std::move(colIndicesToScan), id, paramsString},
          sharedState{std::move(sharedState)} {}

    inline void setMaxMorselSize() override { maxMorselSize = sharedState->getMaxMorselSize(); }
    inline std::unique_ptr<FTableScanMorsel> getMorsel() override {
        return sharedState->getMorsel(maxMorselSize);
    }

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<UnionAllScan>(
            outVecPositions, colIndicesToScan, sharedState, id, paramsString);
    }

private:
    std::shared_ptr<UnionAllScanSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
