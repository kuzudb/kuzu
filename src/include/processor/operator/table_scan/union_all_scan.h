#pragma once

#include "processor/operator/table_scan/base_table_scan.h"

namespace kuzu {
namespace processor {

class UnionAllScanSharedState {

public:
    explicit UnionAllScanSharedState(
        std::vector<std::shared_ptr<FTableSharedState>> fTableSharedStates)
        : fTableSharedStates{std::move(fTableSharedStates)}, fTableToScanIdx{0} {}

    std::unique_ptr<FTableScanMorsel> getMorsel();

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

    inline std::unique_ptr<FTableScanMorsel> getMorsel() override {
        return sharedState->getMorsel();
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
