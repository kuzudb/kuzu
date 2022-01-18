#pragma once

#include "src/processor/include/physical_plan/operator/aggregate/base_aggregate.h"

namespace graphflow {
namespace processor {

class SimpleAggregateSharedState : public BaseAggregateSharedState {

public:
    explicit SimpleAggregateSharedState(
        const vector<unique_ptr<AggregateFunction>>& aggregateFunctions);

    void combineAggregateStates(const vector<unique_ptr<AggregateState>>& localAggregateStates);

    void finalizeAggregateStates();

    bool hasMoreToRead() override;

    pair<uint64_t, uint64_t> getNextRangeToRead() override;

    inline AggregateState* getAggregateState(uint64_t idx) {
        return globalAggregateStates[idx].get();
    }

private:
    vector<unique_ptr<AggregateState>> globalAggregateStates;
};

class SimpleAggregate : public BaseAggregate {

public:
    SimpleAggregate(shared_ptr<SimpleAggregateSharedState> sharedState,
        vector<DataPos> aggregateVectorsPos,
        vector<unique_ptr<AggregateFunction>> aggregateFunctions,
        unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id);

    void execute() override;

    void finalize() override;

    unique_ptr<PhysicalOperator> clone() override;

private:
    shared_ptr<SimpleAggregateSharedState> sharedState;
    vector<unique_ptr<AggregateState>> localAggregateStates;
};

} // namespace processor
} // namespace graphflow
