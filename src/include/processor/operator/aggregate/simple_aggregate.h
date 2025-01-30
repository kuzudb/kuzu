#pragma once

#include "processor/operator/aggregate/aggregate_hash_table.h"
#include "processor/operator/aggregate/base_aggregate.h"

namespace kuzu {
namespace processor {

// NOLINTNEXTLINE(cppcoreguidelines-virtual-class-destructor): This is a final class.
class SimpleAggregateSharedState final : public BaseAggregateSharedState {
public:
    explicit SimpleAggregateSharedState(
        const std::vector<function::AggregateFunction>& aggregateFunctions);

    void combineAggregateStates(
        const std::vector<std::unique_ptr<function::AggregateState>>& localAggregateStates,
        storage::MemoryManager* memoryManager);

    void finalizeAggregateStates();

    std::pair<uint64_t, uint64_t> getNextRangeToRead() override;

    function::AggregateState* getAggregateState(uint64_t idx) {
        return globalAggregateStates[idx].get();
    }

private:
    std::vector<std::unique_ptr<function::AggregateState>> globalAggregateStates;
};

struct SimpleAggregatePrintInfo final : OPPrintInfo {
    binder::expression_vector aggregates;

    explicit SimpleAggregatePrintInfo(binder::expression_vector aggregates)
        : aggregates{std::move(aggregates)} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<SimpleAggregatePrintInfo>(new SimpleAggregatePrintInfo(*this));
    }

private:
    SimpleAggregatePrintInfo(const SimpleAggregatePrintInfo& other)
        : OPPrintInfo{other}, aggregates{other.aggregates} {}
};

class SimpleAggregate final : public BaseAggregate {
public:
    SimpleAggregate(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::shared_ptr<SimpleAggregateSharedState> sharedState,
        std::vector<function::AggregateFunction> aggregateFunctions,
        std::vector<AggregateInfo> aggInfos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : BaseAggregate{std::move(resultSetDescriptor), std::move(aggregateFunctions),
              std::move(aggInfos), std::move(child), id, std::move(printInfo)},
          sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    void finalizeInternal(ExecutionContext* /*context*/) override {
        sharedState->finalizeAggregateStates();
        if (metrics) {
            metrics->numOutputTuple.incrementByOne();
        }
    }

    std::unique_ptr<PhysicalOperator> copy() override {
        return make_unique<SimpleAggregate>(resultSetDescriptor->copy(), sharedState,
            copyVector(aggregateFunctions), copyVector(aggInfos), children[0]->copy(), id,
            printInfo->copy());
    }

    // TODO(bmwinger): We can use the same type of partitioning to handle distinct simple aggregates
    // It could even be the exact same pipeline, but it would perform better if we don't use the
    // hash tables for anything but collecting the distinct values
    // Maybe try and move the partitioning into BaseAggregate
    bool isParallel() const final { return !containDistinctAggregate(); }

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
