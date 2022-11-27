#pragma once

#include "processor/operator/aggregate/aggregate_hash_table.h"
#include "processor/operator/aggregate/base_aggregate.h"

namespace kuzu {
namespace processor {

class SimpleAggregateSharedState : public BaseAggregateSharedState {

public:
    explicit SimpleAggregateSharedState(
        const vector<unique_ptr<AggregateFunction>>& aggregateFunctions);

    void combineAggregateStates(const vector<unique_ptr<AggregateState>>& localAggregateStates);

    void finalizeAggregateStates();

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
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString);

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    inline void finalize(ExecutionContext* context) override {
        sharedState->finalizeAggregateStates();
    }

    unique_ptr<PhysicalOperator> clone() override;

private:
    shared_ptr<SimpleAggregateSharedState> sharedState;
    vector<unique_ptr<AggregateState>> localAggregateStates;
    vector<unique_ptr<AggregateHashTable>> distinctHashTables;
};

} // namespace processor
} // namespace kuzu
