#pragma once

#include "src/processor/include/physical_plan/operator/aggregate/simple_aggregate.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/source_operator.h"

using namespace graphflow::function;

namespace graphflow {
namespace processor {

class SimpleAggregateScan : public PhysicalOperator, public SourceOperator {

public:
    SimpleAggregateScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        shared_ptr<AggregateSharedState> sharedState, vector<DataPos> aggregatesOutputPos,
        vector<DataType> aggregatesDataType, unique_ptr<PhysicalOperator> child,
        ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(child), context, id}, SourceOperator{move(resultSetDescriptor)},
          sharedState{move(sharedState)}, aggregatesOutputPos{move(aggregatesOutputPos)},
          aggregatesDataType{move(aggregatesDataType)} {}

    // This constructor is used for cloning only.
    SimpleAggregateScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        shared_ptr<AggregateSharedState> sharedState, vector<DataPos> aggregatesOutputPos,
        vector<DataType> aggregatesDataType, ExecutionContext& context, uint32_t id)
        : PhysicalOperator{context, id}, SourceOperator{move(resultSetDescriptor)},
          sharedState{move(sharedState)}, aggregatesOutputPos{move(aggregatesOutputPos)},
          aggregatesDataType{move(aggregatesDataType)} {}

    PhysicalOperatorType getOperatorType() override { return AGGREGATE_SCAN; }

    shared_ptr<ResultSet> initResultSet() override;

    bool getNextTuples() override;

    // SimpleAggregateScan is the source operator of a pipeline, so it should not clone its child.
    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<SimpleAggregateScan>(resultSetDescriptor->copy(), sharedState,
            aggregatesOutputPos, aggregatesDataType, context, id);
    }

private:
    shared_ptr<AggregateSharedState> sharedState;

    vector<DataPos> aggregatesOutputPos;
    vector<DataType> aggregatesDataType;

    shared_ptr<DataChunk> outDataChunk;
};
} // namespace processor
} // namespace graphflow
