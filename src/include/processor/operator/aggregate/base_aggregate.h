#pragma once

#include <mutex>

#include "aggregate_input.h"
#include "common/copy_constructors.h"
#include "common/mpsc_queue.h"
#include "function/aggregate_function.h"
#include "processor/operator/sink.h"
#include "processor/result/factorized_table.h"
#include "processor/result/factorized_table_schema.h"

namespace kuzu {
namespace main {
class ClientContext;
}
namespace processor {
class AggregateHashTable;

size_t getNumPartitionsForParallelism(main::ClientContext* context);

// FactorizedTable which stores AggregateStates, which may contain heap-allocated memory that must
// be destroyed
// If you move a tuple containing an AggregateState to another table, the memory
// should be cleared (or the numTuples updated) so that it won't be destroyed when the source table
// is destroyed
class AggregateFactorizedTable : public FactorizedTable {
public:
    AggregateFactorizedTable(storage::MemoryManager* memoryManager, FactorizedTableSchema schema,
        uint32_t aggStateColIdx, uint32_t numAggregateFunctions)
        : FactorizedTable(memoryManager, std::move(schema)), aggStateColIndex{aggStateColIdx},
          numAggregateFunctions{numAggregateFunctions} {}
    ~AggregateFactorizedTable() override;

    std::unique_ptr<AggregateFactorizedTable> createEmptyCopy() const {
        return std::make_unique<AggregateFactorizedTable>(memoryManager, tableSchema.copy(),
            aggStateColIndex, numAggregateFunctions);
    }

private:
    uint32_t aggStateColIndex;
    uint32_t numAggregateFunctions;
};

struct TupleBlock;
class BaseAggregateSharedState {
    friend class BaseAggregate;

public:
    template<typename Partition, typename Func>
    void finalizePartitions(std::vector<Partition>& globalPartitions, Func finalizeFunc) {
        for (auto& partition : globalPartitions) {
            if (!partition.finalized && partition.mtx.try_lock()) {
                if (partition.finalized) {
                    // If there was a data race in the above && a thread may get through after
                    // another thread has finalized this partition Ignore coverage since we can't
                    // reliably test this data race
                    // LCOV_EXCL_START
                    partition.mtx.unlock();
                    continue;
                    // LCOV_EXCL_END
                }
                finalizeFunc(partition);
                partition.finalized = true;
                partition.mtx.unlock();
            }
        }
    }

    bool isReadyForFinalization() const { return readyForFinalization; }

protected:
    explicit BaseAggregateSharedState(
        const std::vector<function::AggregateFunction>& aggregateFunctions, size_t numPartitions);

    virtual std::pair<uint64_t, uint64_t> getNextRangeToRead() = 0;

    ~BaseAggregateSharedState() = default;

    void finalizeAggregateHashTable(const AggregateHashTable& localHashTable);

    class HashTableQueue {
    public:
        explicit HashTableQueue(std::unique_ptr<AggregateFactorizedTable> factorizedTable);

        std::unique_ptr<HashTableQueue> copy() const;
        ~HashTableQueue();

        void moveTuple(std::span<uint8_t> tuple);

        void mergeInto(AggregateHashTable& hashTable);

        bool empty() const;
        common::MPSCQueue<TupleBlock*> queuedTuples;
        // When queueing tuples, they are always added to the headBlock until the headBlock is full
        // (numTuplesReserved >= factorizedTable.getNumTuplesPerBlock()), then pushed into the
        // queuedTuples (at which point, the numTuplesReserved may not be equal to the
        // numTuplesWritten)
        std::atomic<TupleBlock*> headBlock;
        uint64_t numTuplesPerBlock;
    };

protected:
    std::mutex mtx;
    uint64_t currentOffset;
    std::vector<function::AggregateFunction> aggregateFunctions;
    std::atomic<size_t> numThreadsFinishedProducing;
    std::atomic<size_t> numThreads;
    common::MPSCQueue<std::unique_ptr<common::InMemOverflowBuffer>> overflow;
    uint8_t shiftForPartitioning;
    bool readyForFinalization;
};

class BaseAggregate : public Sink {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::AGGREGATE;

protected:
    BaseAggregate(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::shared_ptr<BaseAggregateSharedState> sharedState,
        std::vector<function::AggregateFunction> aggregateFunctions,
        std::vector<AggregateInfo> aggInfos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Sink{std::move(resultSetDescriptor), type_, std::move(child), id, std::move(printInfo)},
          aggregateFunctions{std::move(aggregateFunctions)}, aggInfos{std::move(aggInfos)},
          sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> copy() override = 0;

    bool containDistinctAggregate() const;

    void finalizeInternal(ExecutionContext* /*context*/) override {
        // Delegated to HashAggregateFinalize so it can be parallelized
        sharedState->readyForFinalization = true;
    }

protected:
    std::vector<function::AggregateFunction> aggregateFunctions;
    std::vector<AggregateInfo> aggInfos;
    std::vector<AggregateInput> aggInputs;
    std::shared_ptr<BaseAggregateSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
