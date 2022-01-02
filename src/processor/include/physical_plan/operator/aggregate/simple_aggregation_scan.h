#pragma once

#include "src/processor/include/physical_plan/operator/aggregate/simple_aggregate.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/source_operator.h"

using namespace graphflow::function;

namespace graphflow {
namespace processor {

class SimpleAggregationScan : public PhysicalOperator, public SourceOperator {

public:
    SimpleAggregationScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> outDataPos, unique_ptr<PhysicalOperator> child,
        shared_ptr<AggregationSharedState> sharedState, ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(child), context, id}, SourceOperator{move(resultSetDescriptor)},
          outDataPos{move(outDataPos)}, sharedState{move(sharedState)} {}

    // This constructor is used for cloning only.
    SimpleAggregationScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> outDataPos, shared_ptr<AggregationSharedState> sharedState,
        ExecutionContext& context, uint32_t id)
        : PhysicalOperator{context, id}, SourceOperator{move(resultSetDescriptor)},
          outDataPos{move(outDataPos)}, sharedState{move(sharedState)} {}

    PhysicalOperatorType getOperatorType() override { return AGGREGATION_SCAN; }

    shared_ptr<ResultSet> initResultSet() override;

    bool getNextTuples() override;

    // SimpleAggregationScan is the source operator of a pipeline, so it should not clone its child.
    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<SimpleAggregationScan>(
            resultSetDescriptor->copy(), outDataPos, sharedState, context, id);
    }

private:
    vector<DataPos> outDataPos;
    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<AggregationSharedState> sharedState;
};
} // namespace processor
} // namespace graphflow
