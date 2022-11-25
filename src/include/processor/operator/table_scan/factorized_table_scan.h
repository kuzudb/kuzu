#pragma once

#include "processor/operator/table_scan/base_table_scan.h"

namespace kuzu {
namespace processor {

class FactorizedTableScan : public BaseTableScan {
public:
    // Scan all columns.
    FactorizedTableScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> outVecPositions, vector<DataType> outVecDataTypes,
        vector<uint32_t> colIndicesToScan, shared_ptr<FTableSharedState> sharedState,
        vector<uint64_t> flatDataChunkPositions, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : BaseTableScan{std::move(resultSetDescriptor), std::move(outVecPositions),
              move(outVecDataTypes), std::move(colIndicesToScan), std::move(child), id,
              paramsString},
          sharedState{std::move(sharedState)}, flatDataChunkPositions{
                                                   std::move(flatDataChunkPositions)} {}

    // Scan some columns.
    FactorizedTableScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> outVecPositions, vector<DataType> outVecDataTypes,
        vector<uint32_t> colIndicesToScan, vector<uint64_t> flatDataChunkPositions, uint32_t id,
        const string& paramsString)
        : BaseTableScan{std::move(resultSetDescriptor), std::move(outVecPositions),
              std::move(outVecDataTypes), std::move(colIndicesToScan), id, paramsString},
          flatDataChunkPositions{std::move(flatDataChunkPositions)} {}

    // For clone only.
    FactorizedTableScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> outVecPositions, vector<DataType> outVecDataTypes,
        vector<uint32_t> colIndicesToScan, shared_ptr<FTableSharedState> sharedState,
        vector<uint64_t> flatDataChunkPositions, uint32_t id, const string& paramsString)
        : BaseTableScan{std::move(resultSetDescriptor), std::move(outVecPositions),
              std::move(outVecDataTypes), std::move(colIndicesToScan), id, paramsString},
          sharedState{std::move(sharedState)}, flatDataChunkPositions{
                                                   std::move(flatDataChunkPositions)} {}

    inline void setSharedState(shared_ptr<FTableSharedState> state) {
        sharedState = std::move(state);
    }
    inline void setMaxMorselSize() override { maxMorselSize = sharedState->getMaxMorselSize(); }
    inline unique_ptr<FTableScanMorsel> getMorsel() override {
        return sharedState->getMorsel(maxMorselSize);
    }
    inline PhysicalOperatorType getOperatorType() override { return FACTORIZED_TABLE_SCAN; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    inline unique_ptr<PhysicalOperator> clone() override {
        assert(sharedState != nullptr);
        return make_unique<FactorizedTableScan>(resultSetDescriptor->copy(), outVecPositions,
            outVecDataTypes, colIndicesToScan, sharedState, flatDataChunkPositions, id,
            paramsString);
    }

private:
    shared_ptr<FTableSharedState> sharedState;
    vector<uint64_t> flatDataChunkPositions;
};

} // namespace processor
} // namespace kuzu
