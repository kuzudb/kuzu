#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/source_operator.h"

namespace graphflow {
namespace processor {

class BaseAggregateScan : public PhysicalOperator, public SourceOperator {

public:
    BaseAggregateScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> aggregatesPos, vector<DataType> aggregatesDataType,
        unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(child), context, id}, SourceOperator{move(resultSetDescriptor)},
          aggregatesPos{move(aggregatesPos)}, aggregatesDataType{move(aggregatesDataType)} {}

    BaseAggregateScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> aggregatesPos, vector<DataType> aggregatesDataType,
        ExecutionContext& context, uint32_t id)
        : PhysicalOperator{context, id}, SourceOperator{move(resultSetDescriptor)},
          aggregatesPos{move(aggregatesPos)}, aggregatesDataType{move(aggregatesDataType)} {}

    PhysicalOperatorType getOperatorType() override { return AGGREGATE_SCAN; }

    shared_ptr<ResultSet> initResultSet() override;

    bool getNextTuples() override = 0;

    unique_ptr<PhysicalOperator> clone() override = 0;

protected:
    vector<DataPos> aggregatesPos;
    vector<DataType> aggregatesDataType;
    vector<ValueVector*> aggregatesVector;
    DataChunk* outDataChunk;
};

} // namespace processor
} // namespace graphflow
