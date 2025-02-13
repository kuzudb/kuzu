#pragma once

#include "common/assert.h"
#include "common/in_mem_overflow_buffer.h"
#include "common/mpsc_queue.h"
#include "processor/execution_context.h"
#include "processor/operator/aggregate/aggregate_hash_table.h"
#include "processor/operator/aggregate/base_aggregate.h"
#include "processor/result/factorized_table.h"
#include "processor/result/factorized_table_schema.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace processor {

// NOLINTNEXTLINE(cppcoreguidelines-virtual-class-destructor): This is a final class.
class SimpleAggregateSharedState final : public BaseAggregateSharedState {
    friend class SimpleAggregate;

public:
    explicit SimpleAggregateSharedState(main::ClientContext* clientContext,
        const std::vector<function::AggregateFunction>& aggregateFunctions,
        const std::vector<AggregateInfo>& aggInfos);

    // The partitioningData objects need a stable pointer to this shared state
    DELETE_COPY_AND_MOVE(SimpleAggregateSharedState);

    void combineAggregateStates(
        const std::vector<std::unique_ptr<function::AggregateState>>& localAggregateStates,
        storage::MemoryManager* memoryManager);

    void finalizeAggregateStates();

    std::pair<uint64_t, uint64_t> getNextRangeToRead() override;

    function::AggregateState* getAggregateState(uint64_t idx) {
        return globalAggregateStates[idx].get();
    }

    void finalizeDistinctHashTables(
        const std::vector<std::unique_ptr<PartitioningAggregateHashTable>>& localDistinctTables);

protected:
    struct Partition {
        std::mutex mtx;
        std::vector<
            std::tuple<std::unique_ptr<AggregateHashTable>, std::unique_ptr<HashTableQueue>>>
            distinctTables;
        std::atomic<bool> finalized = false;
    };

    class SimpleAggregatePartitioningData : public AggregatePartitioningData {
    public:
        SimpleAggregatePartitioningData(SimpleAggregateSharedState* sharedState, size_t functionIdx)
            : sharedState{sharedState}, functionIdx{functionIdx} {}

        void appendTuples(const FactorizedTable& factorizedTable,
            ft_col_offset_t hashOffset) override {
            KU_ASSERT(sharedState->globalPartitions.size() > 0);
            auto numBytesPerTuple = factorizedTable.getTableSchema()->getNumBytesPerTuple();
            for (ft_tuple_idx_t tupleIdx = 0; tupleIdx < factorizedTable.getNumTuples();
                 tupleIdx++) {
                auto tuple = factorizedTable.getTuple(tupleIdx);
                auto hash = *reinterpret_cast<common::hash_t*>(tuple + hashOffset);
                auto& partition =
                    sharedState->globalPartitions[(hash >> sharedState->shiftForPartitioning) %
                                                  sharedState->globalPartitions.size()];
                std::get<1>(partition.distinctTables[functionIdx])
                    ->appendTuple(std::span(tuple, numBytesPerTuple));
            }
        }

        void appendDistinctTuple(size_t, std::span<uint8_t>, common::hash_t) override {
            KU_UNREACHABLE;
        }

        void appendOverflow(common::InMemOverflowBuffer&& overflowBuffer) override {
            sharedState->overflow.push(
                std::make_unique<common::InMemOverflowBuffer>(std::move(overflowBuffer)));
        }

    private:
        SimpleAggregateSharedState* sharedState;
        size_t functionIdx;
    };

private:
    common::MPSCQueue<std::unique_ptr<common::InMemOverflowBuffer>> overflow;
    std::vector<Partition> globalPartitions;
    std::vector<SimpleAggregatePartitioningData> partitioningData;
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
          sharedState{std::move(sharedState)}, hasDistinct{containDistinctAggregate()} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    void updateDistinctAggregateStates(storage::MemoryManager* memoryManager);

    void finalizeInternal(ExecutionContext* context) override {
        updateDistinctAggregateStates(context->clientContext->getMemoryManager());
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

private:
    void computeDistinctAggregate(AggregateHashTable* distinctHT,
        function::AggregateFunction* function, AggregateInput* input,
        function::AggregateState* state, storage::MemoryManager* memoryManager);
    void computeAggregate(function::AggregateFunction* function, AggregateInput* input,
        function::AggregateState* state, storage::MemoryManager* memoryManager);

    bool containDistinctAggregate() const {
        for (auto& function : aggregateFunctions) {
            if (function.isFunctionDistinct()) {
                return true;
            }
        }
        return false;
    }

private:
    std::shared_ptr<SimpleAggregateSharedState> sharedState;
    std::vector<std::unique_ptr<function::AggregateState>> localAggregateStates;
    std::vector<std::unique_ptr<PartitioningAggregateHashTable>> distinctHashTables;
    bool hasDistinct;
};

} // namespace processor
} // namespace kuzu
