#pragma once

#include "processor/operator/result_collector.h"

namespace kuzu {
namespace processor {

class BaseTableScan : public PhysicalOperator {
protected:
    // For factorized table scan of all columns
    BaseTableScan(PhysicalOperatorType operatorType, std::vector<DataPos> outVecPositions,
        std::vector<uint32_t> colIndicesToScan, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{operatorType, std::move(child), id, paramsString},
          outVecPositions{std::move(outVecPositions)}, colIndicesToScan{
                                                           std::move(colIndicesToScan)} {}

    // For factorized table scan of some columns
    BaseTableScan(PhysicalOperatorType operatorType, std::vector<DataPos> outVecPositions,
        std::vector<uint32_t> colIndicesToScan, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{operatorType, id, paramsString},
          outVecPositions{std::move(outVecPositions)}, colIndicesToScan{
                                                           std::move(colIndicesToScan)} {}

    // For union all scan
    BaseTableScan(PhysicalOperatorType operatorType, std::vector<DataPos> outVecPositions,
        std::vector<uint32_t> colIndicesToScan,
        std::vector<std::unique_ptr<PhysicalOperator>> children, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator{operatorType, std::move(children), id, paramsString},
          outVecPositions{std::move(outVecPositions)}, colIndicesToScan{
                                                           std::move(colIndicesToScan)} {}

    inline bool isSource() const override { return true; }

    virtual std::unique_ptr<FTableScanMorsel> getMorsel() = 0;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

protected:
    std::vector<DataPos> outVecPositions;
    std::vector<uint32_t> colIndicesToScan;

    std::vector<common::ValueVector*> vectorsToScan;
};

} // namespace processor
} // namespace kuzu
