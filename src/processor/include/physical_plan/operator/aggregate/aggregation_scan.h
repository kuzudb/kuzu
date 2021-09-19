#pragma once

#include "src/processor/include/physical_plan/operator/aggregate/aggregate.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

using namespace graphflow::function;

namespace graphflow {
namespace processor {

class AggregationScan : public PhysicalOperator {

public:
    AggregationScan(const vector<DataPos>& outDataPos, unique_ptr<PhysicalOperator> prevOperator,
        shared_ptr<AggregationSharedState> sharedState, ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(prevOperator), AGGREGATION_SCAN, context, id}, outDataChunkPos{0},
          outDataPos{outDataPos}, sharedState{move(sharedState)} {}

    void initResultSet(const shared_ptr<ResultSet>& resultSet) override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AggregationScan>(
            outDataPos, prevOperator->clone(), move(sharedState), context, id);
    }

private:
    uint32_t outDataChunkPos;
    vector<DataPos> outDataPos;
    shared_ptr<AggregationSharedState> sharedState;
};
} // namespace processor
} // namespace graphflow
