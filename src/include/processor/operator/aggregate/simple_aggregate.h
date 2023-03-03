#pragma once

#include "processor/operator/aggregate/aggregate_hash_table.h"
#include "processor/operator/aggregate/base_aggregate.h"

namespace kuzu {
namespace processor {

class SimpleAggregateSharedState : public BaseAggregateSharedState {

public:
    explicit SimpleAggregateSharedState(
        const std::vector<std::unique_ptr<function::AggregateFunction>>& aggregateFunctions);

    void combineAggregateStates(
        const std::vector<std::unique_ptr<function::AggregateState>>& localAggregateStates,
        storage::MemoryManager* memoryManager);

    void finalizeAggregateStates();

    std::pair<uint64_t, uint64_t> getNextRangeToRead() override;

    inline function::AggregateState* getAggregateState(uint64_t idx) {
        return globalAggregateStates[idx].get();
    }

private:
    std::vector<std::unique_ptr<function::AggregateState>> globalAggregateStates;
};

class SimpleAggregate : public BaseAggregate {
public:
    SimpleAggregate(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::shared_ptr<SimpleAggregateSharedState> sharedState,
        std::vector<DataPos> aggregateVectorsPos,
        std::vector<std::unique_ptr<function::AggregateFunction>> aggregateFunctions,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : BaseAggregate{std::move(resultSetDescriptor), std::move(aggregateVectorsPos),
              std::move(aggregateFunctions), std::move(child), id, paramsString},
          sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    inline void finalize(ExecutionContext* context) override {
        sharedState->finalizeAggregateStates();
    }

    std::unique_ptr<PhysicalOperator> clone() override;

private:
    std::shared_ptr<SimpleAggregateSharedState> sharedState;
    std::vector<std::unique_ptr<function::AggregateState>> localAggregateStates;
    std::vector<std::unique_ptr<AggregateHashTable>> distinctHashTables;
};

} // namespace processor
} // namespace kuzu
