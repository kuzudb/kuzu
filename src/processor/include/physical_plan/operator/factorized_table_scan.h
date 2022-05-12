#pragma once

#include "src/processor/include/physical_plan/operator/base_table_scan.h"

namespace graphflow {
namespace processor {

class FactorizedTableScan : public BaseTableScan {

public:
    FactorizedTableScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> outVecPositions, vector<DataType> outVecDataTypes,
        shared_ptr<FTableSharedState> sharedState, unique_ptr<PhysicalOperator> child, uint32_t id)
        : BaseTableScan{move(resultSetDescriptor), move(outVecPositions), move(outVecDataTypes),
              move(child), id},
          sharedState{move(sharedState)} {}

    // For clone only
    FactorizedTableScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> outVecPositions, vector<DataType> outVecDataTypes,
        shared_ptr<FTableSharedState> sharedState, uint32_t id)
        : BaseTableScan{move(resultSetDescriptor), move(outVecPositions), move(outVecDataTypes),
              id},
          sharedState{move(sharedState)} {}

    void setMaxMorselSize() override { sharedState->getMaxMorselSize(); }
    unique_ptr<FTableScanMorsel> getMorsel() override {
        return sharedState->getMorsel(maxMorselSize);
    }

    PhysicalOperatorType getOperatorType() override { return FACTORIZED_TABLE_SCAN; }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<FactorizedTableScan>(
            resultSetDescriptor->copy(), outVecPositions, outVecDataTypes, sharedState, id);
    }

private:
    shared_ptr<FTableSharedState> sharedState;
};

} // namespace processor
} // namespace graphflow
