#pragma once

#include "src/processor/include/physical_plan/operator/base_table_scan.h"

namespace graphflow {
namespace processor {

class FactorizedTableScan : public BaseTableScan {

public:
    FactorizedTableScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> outVecPositions, vector<DataType> outVecDataTypes,
        shared_ptr<FTableSharedState> sharedState, vector<uint64_t> flatDataChunkPositions,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : BaseTableScan{move(resultSetDescriptor), move(outVecPositions), move(outVecDataTypes),
              move(child), id, paramsString},
          sharedState{move(sharedState)}, flatDataChunkPositions{move(flatDataChunkPositions)} {}

    // For clone only
    FactorizedTableScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> outVecPositions, vector<DataType> outVecDataTypes,
        shared_ptr<FTableSharedState> sharedState, vector<uint64_t> flatDataChunkPositions,
        uint32_t id, const string& paramsString)
        : BaseTableScan{move(resultSetDescriptor), move(outVecPositions), move(outVecDataTypes), id,
              paramsString},
          sharedState{move(sharedState)}, flatDataChunkPositions{move(flatDataChunkPositions)} {}

    void setMaxMorselSize() override { maxMorselSize = sharedState->getMaxMorselSize(); }
    unique_ptr<FTableScanMorsel> getMorsel() override {
        return sharedState->getMorsel(maxMorselSize);
    }

    PhysicalOperatorType getOperatorType() override { return FACTORIZED_TABLE_SCAN; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override {
        PhysicalOperator::init(context);
        resultSet = populateResultSet();
        for (auto& pos : flatDataChunkPositions) {
            resultSet->dataChunks[pos]->state = DataChunkState::getSingleValueDataChunkState();
        }
        initFurther(context);
        return resultSet;
    }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<FactorizedTableScan>(resultSetDescriptor->copy(), outVecPositions,
            outVecDataTypes, sharedState, flatDataChunkPositions, id, paramsString);
    }

private:
    shared_ptr<FTableSharedState> sharedState;
    vector<uint64_t> flatDataChunkPositions;
};

} // namespace processor
} // namespace graphflow
