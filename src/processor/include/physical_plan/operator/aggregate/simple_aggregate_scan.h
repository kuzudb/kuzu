#pragma once

#include "src/processor/include/physical_plan/operator/aggregate/base_aggregate_scan.h"
#include "src/processor/include/physical_plan/operator/aggregate/simple_aggregate.h"

namespace graphflow {
namespace processor {

class SimpleAggregateScan : public BaseAggregateScan {

public:
    SimpleAggregateScan(shared_ptr<SimpleAggregateSharedState> sharedState,
        unique_ptr<ResultSetDescriptor> resultSetDescriptor, vector<DataPos> aggregatesPos,
        vector<DataType> aggregatesDataType, unique_ptr<PhysicalOperator> child,
        ExecutionContext& context, uint32_t id)
        : BaseAggregateScan{move(resultSetDescriptor), move(aggregatesPos),
              move(aggregatesDataType), move(child), context, id},
          sharedState{move(sharedState)} {}

    // This constructor is used for cloning only.
    SimpleAggregateScan(shared_ptr<SimpleAggregateSharedState> sharedState,
        unique_ptr<ResultSetDescriptor> resultSetDescriptor, vector<DataPos> aggregatesPos,
        vector<DataType> aggregatesDataType, ExecutionContext& context, uint32_t id)
        : BaseAggregateScan{move(resultSetDescriptor), move(aggregatesPos),
              move(aggregatesDataType), context, id},
          sharedState{move(sharedState)} {}

    bool getNextTuples() override;

    // SimpleAggregateScan is the source operator of a pipeline, so it should not clone its child.
    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<SimpleAggregateScan>(sharedState, resultSetDescriptor->copy(),
            aggregatesPos, aggregatesDataType, context, id);
    }

private:
    shared_ptr<SimpleAggregateSharedState> sharedState;
};

} // namespace processor
} // namespace graphflow
