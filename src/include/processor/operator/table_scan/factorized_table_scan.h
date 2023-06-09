#pragma once

#include "processor/operator/table_scan/base_table_scan.h"

namespace kuzu {
namespace processor {

class FactorizedTableScan : public BaseTableScan {
public:
    // Scan all columns.
    FactorizedTableScan(std::vector<DataPos> outVecPositions,
        std::vector<uint32_t> colIndicesToScan, std::shared_ptr<FTableSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : BaseTableScan{PhysicalOperatorType::FACTORIZED_TABLE_SCAN, std::move(outVecPositions),
              std::move(colIndicesToScan), std::move(child), id, paramsString},
          sharedState{std::move(sharedState)} {}

    // Scan some columns.
    FactorizedTableScan(std::vector<DataPos> outVecPositions,
        std::vector<uint32_t> colIndicesToScan, uint32_t id, const std::string& paramsString)
        : BaseTableScan{PhysicalOperatorType::FACTORIZED_TABLE_SCAN, std::move(outVecPositions),
              std::move(colIndicesToScan), id, paramsString} {}

    // For clone only.
    FactorizedTableScan(std::vector<DataPos> outVecPositions,
        std::vector<uint32_t> colIndicesToScan, std::shared_ptr<FTableSharedState> sharedState,
        uint32_t id, const std::string& paramsString)
        : BaseTableScan{PhysicalOperatorType::FACTORIZED_TABLE_SCAN, std::move(outVecPositions),
              std::move(colIndicesToScan), id, paramsString},
          sharedState{std::move(sharedState)} {}

    inline std::unique_ptr<FTableScanMorsel> getMorsel() override {
        return sharedState->getMorsel();
    }

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<FactorizedTableScan>(
            outVecPositions, colIndicesToScan, sharedState, id, paramsString);
    }

private:
    std::shared_ptr<FTableSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
