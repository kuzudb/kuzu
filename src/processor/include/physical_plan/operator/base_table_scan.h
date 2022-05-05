#pragma once

#include "src/processor/include/physical_plan/operator/result_collector.h"
#include "src/processor/include/physical_plan/operator/source_operator.h"

namespace graphflow {
namespace processor {

class BaseTableScan : public PhysicalOperator, public SourceOperator {

protected:
    // For factorized table scan
    BaseTableScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> outVecPositions, vector<DataType> outVecDataTypes,
        unique_ptr<PhysicalOperator> child, uint32_t id)
        : PhysicalOperator{move(child), id}, SourceOperator{move(resultSetDescriptor)},
          outVecPositions{move(outVecPositions)}, outVecDataTypes{move(outVecDataTypes)} {}

    // For union all scan
    BaseTableScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> outVecPositions, vector<DataType> outVecDataTypes,
        vector<unique_ptr<PhysicalOperator>> children, uint32_t id)
        : PhysicalOperator{move(children), id}, SourceOperator{move(resultSetDescriptor)},
          outVecPositions{move(outVecPositions)}, outVecDataTypes{move(outVecDataTypes)} {}

    // For clone only
    BaseTableScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> outVecPositions, vector<DataType> outVecDataTypes, uint32_t id)
        : PhysicalOperator{id}, SourceOperator{move(resultSetDescriptor)},
          outVecPositions{move(outVecPositions)}, outVecDataTypes{move(outVecDataTypes)} {}

    virtual void setMaxMorselSize() = 0;
    virtual unique_ptr<FTableScanMorsel> getMorsel() = 0;

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override final;

protected:
    uint64_t maxMorselSize;
    vector<DataPos> outVecPositions;
    vector<DataType> outVecDataTypes;

    vector<shared_ptr<ValueVector>> vectorsToScan;
};

} // namespace processor
} // namespace graphflow
