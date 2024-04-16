#pragma once

#include "processor/operator/aggregate/aggregate_hash_table.h"
#include "processor/operator/aggregate/base_aggregate.h"

namespace kuzu {
namespace processor {

// NOLINTNEXTLINE(cppcoreguidelines-virtual-class-destructor): This is a final class.
class SimpleAggregateSharedState final : public BaseAggregateSharedState {
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
        std::vector<std::unique_ptr<function::AggregateFunction>> aggregateFunctions,
        std::vector<AggregateInfo> aggInfos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramsString)
        : BaseAggregate{std::move(resultSetDescriptor), std::move(aggregateFunctions),
              std::move(aggInfos), std::move(child), id, paramsString},
          sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    inline void finalize(ExecutionContext* /*context*/) override {
        sharedState->finalizeAggregateStates();
    }

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<SimpleAggregate>(resultSetDescriptor->copy(), sharedState,
            cloneAggFunctions(), copyVector(aggInfos), children[0]->clone(), id, paramsString);
    }

private:
    void computeDistinctAggregate(AggregateHashTable* distinctHT,
        function::AggregateFunction* function, AggregateInput* input,
        function::AggregateState* state, storage::MemoryManager* memoryManager);
    void computeAggregate(function::AggregateFunction* function, AggregateInput* input,
        function::AggregateState* state, storage::MemoryManager* memoryManager);

private:
    std::shared_ptr<SimpleAggregateSharedState> sharedState;
    std::vector<std::unique_ptr<function::AggregateState>> localAggregateStates;
    std::vector<std::unique_ptr<AggregateHashTable>> distinctHashTables;
};

} // namespace processor
} // namespace kuzu
