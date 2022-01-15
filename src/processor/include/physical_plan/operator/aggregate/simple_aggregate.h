#pragma once

#include "src/function/include/aggregate/aggregate_function.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/sink.h"

using namespace graphflow::function;

namespace graphflow {
namespace processor {

struct AggregateSharedState {

    explicit AggregateSharedState(vector<unique_ptr<AggregateState>> aggregateStates)
        : aggregateStates{move(aggregateStates)}, numGroups{1}, currentGroupOffset{0} {}

    mutex aggregateSharedStateLock;
    vector<unique_ptr<AggregateState>> aggregateStates;
    uint64_t numGroups;
    uint64_t currentGroupOffset;
};

class SimpleAggregate : public Sink {

public:
    SimpleAggregate(shared_ptr<AggregateSharedState> sharedState,
        vector<DataPos> aggregateVectorsPos,
        vector<unique_ptr<AggregateFunction>> aggregateFunctions,
        unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id);

    PhysicalOperatorType getOperatorType() override { return AGGREGATE; }

    shared_ptr<ResultSet> initResultSet() override;

    void execute() override;

    unique_ptr<PhysicalOperator> clone() override;

    void finalize() override;

private:
    shared_ptr<AggregateSharedState> sharedState;

    vector<DataPos> aggregateVectorsPos;
    vector<ValueVector*> aggregateVectors;
    vector<unique_ptr<AggregateFunction>> aggregateFunctions;
    vector<unique_ptr<AggregateState>> aggregateStates;
};

} // namespace processor
} // namespace graphflow
