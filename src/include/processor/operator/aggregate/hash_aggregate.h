#pragma once

#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "aggregate_hash_table.h"
#include "common/copy_constructors.h"
#include "common/in_mem_overflow_buffer.h"
#include "common/mpsc_queue.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "main/client_context.h"
#include "processor/operator/aggregate/aggregate_input.h"
#include "processor/operator/aggregate/base_aggregate.h"
#include "processor/operator/physical_operator.h"
#include "processor/result/factorized_table.h"
#include "processor/result/factorized_table_schema.h"

namespace kuzu {
namespace processor {

struct HashAggregateInfo {
    std::vector<DataPos> flatKeysPos;
    std::vector<DataPos> unFlatKeysPos;
    std::vector<DataPos> dependentKeysPos;
    FactorizedTableSchema tableSchema;

    HashAggregateInfo(std::vector<DataPos> flatKeysPos, std::vector<DataPos> unFlatKeysPos,
        std::vector<DataPos> dependentKeysPos, FactorizedTableSchema tableSchema);
    EXPLICIT_COPY_DEFAULT_MOVE(HashAggregateInfo);

private:
    HashAggregateInfo(const HashAggregateInfo& other);
};

// NOLINTNEXTLINE(cppcoreguidelines-virtual-class-destructor): This is a final class.
class HashAggregateSharedState final : public BaseAggregateSharedState {

public:
    explicit HashAggregateSharedState(main::ClientContext* context, HashAggregateInfo hashAggInfo,
        const std::vector<function::AggregateFunction>& aggregateFunctions,
        std::span<AggregateInfo> aggregateInfos, std::vector<common::LogicalType> keyTypes,
        std::vector<common::LogicalType> payloadTypes);

    void appendTuple(std::span<uint8_t> tuple, common::hash_t hash) {
        auto& partition =
            globalPartitions[(hash >> shiftForPartitioning) % globalPartitions.size()];
        partition.queue->appendTuple(tuple);
    }

    void appendDistinctTuple(size_t distinctFuncIndex, std::span<uint8_t> tuple,
        common::hash_t hash) {
        auto& partition =
            globalPartitions[(hash >> shiftForPartitioning) % globalPartitions.size()];
        partition.distinctTableQueues[distinctFuncIndex]->appendTuple(tuple);
    }

    void appendOverflow(common::InMemOverflowBuffer&& overflowBuffer) {
        overflow.push(std::make_unique<common::InMemOverflowBuffer>(std::move(overflowBuffer)));
    }

    void finalizeAggregateHashTable();

    std::pair<uint64_t, uint64_t> getNextRangeToRead() override;

    void scan(std::span<uint8_t*> entries, std::vector<common::ValueVector*>& keyVectors,
        common::offset_t startOffset, common::offset_t numRowsToScan,
        std::vector<uint32_t>& columnIndices);

    uint64_t getNumTuples() const;

    uint64_t getCurrentOffset() const { return currentOffset; }

    void setLimitNumber(uint64_t num) { limitNumber = num; }
    uint64_t getLimitNumber() const { return limitNumber; }

    const FactorizedTableSchema* getTableSchema() const {
        return globalPartitions[0].hashTable->getTableSchema();
    }

    const HashAggregateInfo& getAggregateInfo() const { return aggInfo; }

    void assertFinalized() const;

protected:
    std::tuple<const FactorizedTable*, common::offset_t> getPartitionForOffset(
        common::offset_t offset) const;

public:
    HashAggregateInfo aggInfo;
    common::MPSCQueue<std::unique_ptr<common::InMemOverflowBuffer>> overflow;
    class HashTableQueue {
    public:
        HashTableQueue(storage::MemoryManager* memoryManager, FactorizedTableSchema tableSchema);

        std::unique_ptr<HashTableQueue> copy() const {
            return std::make_unique<HashTableQueue>(headBlock.load()->table.getMemoryManager(),
                headBlock.load()->table.getTableSchema()->copy());
        }
        ~HashTableQueue();

        void appendTuple(std::span<uint8_t> tuple);

        void mergeInto(AggregateHashTable& hashTable);

        bool empty() const {
            auto headBlock = this->headBlock.load();
            return (headBlock == nullptr || headBlock->numTuplesReserved == 0) &&
                   queuedTuples.approxSize() == 0;
        }

    private:
        struct TupleBlock {
            TupleBlock(storage::MemoryManager* memoryManager, FactorizedTableSchema tableSchema)
                : numTuplesReserved{0}, numTuplesWritten{0},
                  table{memoryManager, std::move(tableSchema)} {
                // Start at a fixed capacity of one full block (so that concurrent writes are safe).
                // If it is not filled, we resize it to the actual capacity before writing it to the
                // hashTable
                table.resize(table.getNumTuplesPerBlock());
            }
            // numTuplesReserved may be greater than the capacity of the factorizedTable
            // if threads try to write to it while a new block is being allocated
            // So it should not be relied on for anything other than reserving tuples
            std::atomic<uint64_t> numTuplesReserved;
            // Set after the tuple has been written to the block.
            // Once numTuplesWritten == factorizedTable.getNumTuplesPerBlock() all writes have
            // finished
            std::atomic<uint64_t> numTuplesWritten;
            FactorizedTable table;
        };
        common::MPSCQueue<TupleBlock*> queuedTuples;
        // When queueing tuples, they are always added to the headBlock until the headBlock is full
        // (numTuplesReserved >= factorizedTable.getNumTuplesPerBlock()), then pushed into the
        // queuedTuples (at which point, the numTuplesReserved may not be equal to the
        // numTuplesWritten)
        std::atomic<TupleBlock*> headBlock;
    };
    struct Partition {
        std::unique_ptr<AggregateHashTable> hashTable;
        std::mutex mtx;
        std::unique_ptr<HashTableQueue> queue;
        // The tables storing the distinct values for distinct aggregate functions all get merged in
        // the same way as the main table
        std::vector<std::unique_ptr<HashTableQueue>> distinctTableQueues;
        std::atomic<bool> finalized = false;
    };
    std::vector<Partition> globalPartitions;
    uint64_t limitNumber;
    storage::MemoryManager* memoryManager;
    uint8_t shiftForPartitioning;
    bool readyForFinalization = false;
};

struct HashAggregateLocalState {
    std::vector<common::ValueVector*> flatKeyVectors;
    std::vector<common::ValueVector*> unFlatKeyVectors;
    std::vector<common::ValueVector*> dependentKeyVectors;
    common::DataChunkState* leadingState = nullptr;
    std::unique_ptr<PartitioningAggregateHashTable> aggregateHashTable;

    void init(HashAggregateSharedState* sharedState, ResultSet& resultSet,
        main::ClientContext* context, std::vector<function::AggregateFunction>& aggregateFunctions,
        std::vector<common::LogicalType> types);
    uint64_t append(const std::vector<AggregateInput>& aggregateInputs,
        uint64_t multiplicity) const;
};

struct HashAggregatePrintInfo final : OPPrintInfo {
    binder::expression_vector keys;
    binder::expression_vector aggregates;
    uint64_t limitNum;

    HashAggregatePrintInfo(binder::expression_vector keys, binder::expression_vector aggregates)
        : keys{std::move(keys)}, aggregates{std::move(aggregates)}, limitNum{UINT64_MAX} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<HashAggregatePrintInfo>(new HashAggregatePrintInfo(*this));
    }

private:
    HashAggregatePrintInfo(const HashAggregatePrintInfo& other)
        : OPPrintInfo{other}, keys{other.keys}, aggregates{other.aggregates},
          limitNum{other.limitNum} {}
};

class HashAggregate final : public BaseAggregate {
public:
    HashAggregate(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::shared_ptr<HashAggregateSharedState> sharedState,
        std::vector<function::AggregateFunction> aggregateFunctions,
        std::vector<AggregateInfo> aggInfos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : BaseAggregate{std::move(resultSetDescriptor), std::move(aggregateFunctions),
              std::move(aggInfos), std::move(child), id, std::move(printInfo)},
          sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    // Delegated to HashAggregateFinalize so it can be parallelized
    void finalizeInternal(ExecutionContext* /*context*/) override {
        sharedState->readyForFinalization = true;
    }

    std::unique_ptr<PhysicalOperator> copy() override {
        return make_unique<HashAggregate>(resultSetDescriptor->copy(), sharedState,
            copyVector(aggregateFunctions), copyVector(aggInfos), children[0]->copy(), id,
            printInfo->copy());
    }

    std::shared_ptr<HashAggregateSharedState> getSharedState() const { return sharedState; }

private:
    HashAggregateLocalState localState;
    std::shared_ptr<HashAggregateSharedState> sharedState;
};

class HashAggregateFinalize final : public Sink {
public:
    HashAggregateFinalize(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::shared_ptr<HashAggregateSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::AGGREGATE_FINALIZE,
              std::move(child), id, std::move(printInfo)},
          sharedState{std::move(sharedState)} {}

    // Otherwise the runtime metrics for this operator are negative
    // since it doesn't call children[0]->getNextTuple
    bool isSource() const override { return true; }

    void executeInternal(ExecutionContext* /*context*/) override {
        KU_ASSERT(sharedState->readyForFinalization);
        sharedState->finalizeAggregateHashTable();
    }
    void finalizeInternal(ExecutionContext* /*context*/) override {
        sharedState->assertFinalized();
    }

    std::unique_ptr<PhysicalOperator> copy() override {
        return make_unique<HashAggregateFinalize>(resultSetDescriptor->copy(), sharedState,
            children[0]->copy(), id, printInfo->copy());
    }

private:
    std::shared_ptr<HashAggregateSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
