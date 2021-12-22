#pragma once

#include "src/processor/include/physical_plan/operator/aggregate/simple_aggregate.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

using namespace graphflow::function;

namespace graphflow {
namespace processor {

class SimpleAggregationScan : public PhysicalOperator {

public:
    SimpleAggregationScan(const vector<DataPos>& outDataPos,
        unique_ptr<PhysicalOperator> prevOperator, shared_ptr<AggregationSharedState> sharedState,
        ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(prevOperator), AGGREGATION_SCAN, context, id},
          outDataPos{outDataPos}, sharedState{move(sharedState)} {}

    void initResultSet(const shared_ptr<ResultSet>& resultSet) override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<SimpleAggregationScan>(
            outDataPos, prevOperator->clone(), sharedState, context, id);
    }

private:
    vector<DataPos> outDataPos;
    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<AggregationSharedState> sharedState;
};
} // namespace processor
} // namespace graphflow
