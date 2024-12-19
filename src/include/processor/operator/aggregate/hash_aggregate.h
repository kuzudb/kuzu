#pragma once

#include <sys/types.h>

#include <cstdint>
#include <memory>

#include "aggregate_hash_table.h"
#include "common/copy_constructors.h"
#include "common/mpsc_queue.h"
#include "common/vector/value_vector.h"
#include "processor/operator/aggregate/base_aggregate.h"
#include "processor/result/factorized_table.h"
#include "processor/result/factorized_table_schema.h"

namespace kuzu {
namespace processor {

struct HashAggregateInfo;

// NOLINTNEXTLINE(cppcoreguidelines-virtual-class-destructor): This is a final class.
class HashAggregateSharedState final : public BaseAggregateSharedState {

public:
    explicit HashAggregateSharedState(
        const std::vector<function::AggregateFunction>& aggregateFunctions);

    void initPartitions(main::ClientContext* context, std::vector<common::LogicalType> keyDataTypes,
        std::vector<common::LogicalType> payloadDataTypes, HashAggregateInfo& info,
        std::vector<common::LogicalType> types);

    // Will return either the original factorized table for reuse, or a nullptr if the
    std::unique_ptr<FactorizedTable> mergeTable(uint8_t partitionIdx,
        std::unique_ptr<FactorizedTable> aggregateHashTable);

    void tryMergeQueue();
    void finalizeAggregateHashTable();

    std::pair<uint64_t, uint64_t> getNextRangeToRead() override;

    void scan(std::span<uint8_t*> entries, std::vector<common::ValueVector*>& keyVectors,
        common::offset_t startOffset, common::offset_t numRowsToScan,
        std::vector<uint32_t>& columnIndices);

    uint64_t getNumTuples() const;

    uint64_t getCurrentOffset() const { return currentOffset; }

    // return whether limitNumber is exceeded
    bool increaseAndCheckLimitCount(uint64_t num);

    void setLimitNumber(uint64_t num) { limitNumber = num; }

    const FactorizedTableSchema& getTableSchema() const {
        return *globalPartitions[0].hashTable->getFactorizedTable()->getTableSchema();
    }

    void setThreadFinishedProducing() { numThreadsFinishedProducing++; }
    bool allThreadsFinishedProducing() const { return numThreadsFinishedProducing >= numThreads; }

    void registerThread() { numThreads++; }

    void assertFinalized() const;

protected:
    std::tuple<const FactorizedTable*, common::offset_t> getPartitionForOffset(
        common::offset_t offset) const;

private:
    struct Partition {
        std::unique_ptr<AggregateHashTable> hashTable;
        std::mutex mtx;
        common::MPSCQueue<std::unique_ptr<FactorizedTable>> queuedTuples;
        bool finalized = false;
    };
    std::array<Partition, PartitioningAggregateHashTable::NUM_PARTITIONS> globalPartitions;
    std::atomic_uint64_t limitCounter;
    uint64_t limitNumber;
    std::atomic<uint64_t> numThreadsFinishedProducing;
    size_t numThreads;
};

struct HashAggregateInfo {
    std::vector<DataPos> flatKeysPos;
    std::vector<DataPos> unFlatKeysPos;
    std::vector<DataPos> dependentKeysPos;
    FactorizedTableSchema tableSchema;

    HashAggregateInfo(std::vector<DataPos> flatKeysPos, std::vector<DataPos> unFlatKeysPos,
        std::vector<DataPos> dependentKeysPos, FactorizedTableSchema tableSchema);
    HashAggregateInfo(const HashAggregateInfo& other);
};

struct HashAggregateLocalState {
    std::vector<common::ValueVector*> flatKeyVectors;
    std::vector<common::ValueVector*> unFlatKeyVectors;
    std::vector<common::ValueVector*> dependentKeyVectors;
    common::DataChunkState* leadingState = nullptr;
    std::unique_ptr<PartitioningAggregateHashTable> aggregateHashTable;

    void init(HashAggregateSharedState* sharedState, ResultSet& resultSet,
        main::ClientContext* context, HashAggregateInfo& info,
        std::vector<function::AggregateFunction>& aggregateFunctions,
        std::vector<common::LogicalType> types);
    uint64_t append(const std::vector<AggregateInput>& aggregateInputs, uint64_t multiplicity);
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

class HashAggregate : public BaseAggregate {
public:
    HashAggregate(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::shared_ptr<HashAggregateSharedState> sharedState, HashAggregateInfo hashInfo,
        std::vector<function::AggregateFunction> aggregateFunctions,
        std::vector<AggregateInfo> aggInfos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : BaseAggregate{std::move(resultSetDescriptor), std::move(aggregateFunctions),
              std::move(aggInfos), std::move(child), id, std::move(printInfo)},
          hashInfo{std::move(hashInfo)}, sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    void finalizeInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<HashAggregate>(resultSetDescriptor->copy(), sharedState, hashInfo,
            copyVector(aggregateFunctions), copyVector(aggInfos), children[0]->clone(), id,
            printInfo->copy());
    }

    std::shared_ptr<HashAggregateSharedState> getSharedState() const { return sharedState; }

private:
    HashAggregateInfo hashInfo;
    HashAggregateLocalState localState;
    std::shared_ptr<HashAggregateSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
