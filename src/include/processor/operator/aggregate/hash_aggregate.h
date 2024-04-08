#pragma once

#include "aggregate_hash_table.h"
#include "processor/operator/aggregate/base_aggregate.h"

namespace kuzu {
namespace processor {

// NOLINTNEXTLINE(cppcoreguidelines-virtual-class-destructor): This is a final class.
class HashAggregateSharedState final : public BaseAggregateSharedState {

public:
    explicit HashAggregateSharedState(
        const std::vector<std::unique_ptr<function::AggregateFunction>>& aggregateFunctions)
        : BaseAggregateSharedState{aggregateFunctions} {}

    void appendAggregateHashTable(std::unique_ptr<AggregateHashTable> aggregateHashTable);

    void combineAggregateHashTable(storage::MemoryManager& memoryManager);

    void finalizeAggregateHashTable();

    std::pair<uint64_t, uint64_t> getNextRangeToRead() override;

    inline uint8_t* getRow(uint64_t idx) { return globalAggregateHashTable->getEntry(idx); }

    FactorizedTable* getFactorizedTable() { return globalAggregateHashTable->getFactorizedTable(); }

    uint64_t getCurrentOffset() const { return currentOffset; }

private:
    std::vector<std::unique_ptr<AggregateHashTable>> localAggregateHashTables;
    std::unique_ptr<AggregateHashTable> globalAggregateHashTable;
};

struct HashAggregateInfo {
    std::vector<DataPos> flatKeysPos;
    std::vector<DataPos> unFlatKeysPos;
    std::vector<DataPos> dependentKeysPos;
    std::unique_ptr<FactorizedTableSchema> tableSchema;
    HashTableType hashTableType;

    HashAggregateInfo(std::vector<DataPos> flatKeysPos, std::vector<DataPos> unFlatKeysPos,
        std::vector<DataPos> dependentKeysPos, std::unique_ptr<FactorizedTableSchema> tableSchema,
        HashTableType hashTableType);
    HashAggregateInfo(const HashAggregateInfo& other);
};

struct HashAggregateLocalState {
    std::vector<common::ValueVector*> flatKeyVectors;
    std::vector<common::ValueVector*> unFlatKeyVectors;
    std::vector<common::ValueVector*> dependentKeyVectors;
    common::DataChunkState* leadingState;
    std::unique_ptr<AggregateHashTable> aggregateHashTable;

    void init(ResultSet& resultSet, main::ClientContext* context, HashAggregateInfo& info,
        std::vector<std::unique_ptr<function::AggregateFunction>>& aggregateFunctions);
    void append(std::vector<std::unique_ptr<AggregateInput>>& aggregateInputs,
        uint64_t multiplicity) const;
};

class HashAggregate : public BaseAggregate {
public:
    HashAggregate(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::shared_ptr<HashAggregateSharedState> sharedState, HashAggregateInfo aggregateInfo,
        std::vector<std::unique_ptr<function::AggregateFunction>> aggregateFunctions,
        std::vector<std::unique_ptr<AggregateInputInfo>> aggregateInputInfos,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : BaseAggregate{std::move(resultSetDescriptor), std::move(aggregateFunctions),
              std::move(aggregateInputInfos), std::move(child), id, paramsString},
          aggregateInfo{std::move(aggregateInfo)}, sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    void finalize(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<HashAggregate>(resultSetDescriptor->copy(), sharedState, aggregateInfo,
            cloneAggFunctions(), cloneAggInputInfos(), children[0]->clone(), id, paramsString);
    }

private:
    HashAggregateInfo aggregateInfo;
    HashAggregateLocalState localState;
    std::shared_ptr<HashAggregateSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
