#pragma once

#include "processor/operator/result_collector.h"

namespace kuzu {
namespace processor {

class BaseTableScan : public PhysicalOperator {
protected:
    // For factorized table scan of all columns
    BaseTableScan(PhysicalOperatorType operatorType, vector<DataPos> outVecPositions,
        vector<uint32_t> colIndicesToScan, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{operatorType, std::move(child), id, paramsString}, maxMorselSize{0},
          outVecPositions{std::move(outVecPositions)}, colIndicesToScan{
                                                           std::move(colIndicesToScan)} {}

    // For factorized table scan of some columns
    BaseTableScan(PhysicalOperatorType operatorType, vector<DataPos> outVecPositions,
        vector<uint32_t> colIndicesToScan, uint32_t id, const string& paramsString)
        : PhysicalOperator{operatorType, id, paramsString}, maxMorselSize{0},
          outVecPositions{std::move(outVecPositions)}, colIndicesToScan{
                                                           std::move(colIndicesToScan)} {}

    // For union all scan
    BaseTableScan(PhysicalOperatorType operatorType, vector<DataPos> outVecPositions,
        vector<uint32_t> colIndicesToScan, vector<unique_ptr<PhysicalOperator>> children,
        uint32_t id, const string& paramsString)
        : PhysicalOperator{operatorType, std::move(children), id, paramsString}, maxMorselSize{0},
          outVecPositions{std::move(outVecPositions)}, colIndicesToScan{
                                                           std::move(colIndicesToScan)} {}

    inline bool isSource() const override { return true; }

    virtual void setMaxMorselSize() = 0;
    virtual unique_ptr<FTableScanMorsel> getMorsel() = 0;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

protected:
    uint64_t maxMorselSize;
    vector<DataPos> outVecPositions;
    vector<uint32_t> colIndicesToScan;

    vector<shared_ptr<ValueVector>> vectorsToScan;
};

} // namespace processor
} // namespace kuzu
