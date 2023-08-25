#pragma once

#include "aggregate_hash_table.h"
#include "processor/operator/aggregate/base_aggregate.h"

namespace kuzu {
namespace processor {

class HashAggregateSharedState : public BaseAggregateSharedState {

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

private:
    std::vector<std::unique_ptr<AggregateHashTable>> localAggregateHashTables;
    std::unique_ptr<AggregateHashTable> globalAggregateHashTable;
};

class HashAggregate : public BaseAggregate {
public:
    HashAggregate(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::shared_ptr<HashAggregateSharedState> sharedState, std::vector<DataPos> flatKeysPos,
        std::vector<DataPos> unFlatKeysPos, std::vector<DataPos> dependentKeysPos,
        std::vector<std::unique_ptr<function::AggregateFunction>> aggregateFunctions,
        std::vector<std::unique_ptr<AggregateInputInfo>> aggregateInputInfos,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : BaseAggregate{std::move(resultSetDescriptor), std::move(aggregateFunctions),
              std::move(aggregateInputInfos), std::move(child), id, paramsString},
          flatKeysPos{std::move(flatKeysPos)}, unFlatKeysPos{std::move(unFlatKeysPos)},
          dependentKeysPos{std::move(dependentKeysPos)}, sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    void finalize(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<HashAggregate>(resultSetDescriptor->copy(), sharedState, flatKeysPos,
            unFlatKeysPos, dependentKeysPos, cloneAggFunctions(), cloneAggInputInfos(),
            children[0]->clone(), id, paramsString);
    }

private:
    std::vector<DataPos> flatKeysPos;
    std::vector<DataPos> unFlatKeysPos;
    std::vector<DataPos> dependentKeysPos;

    std::vector<common::ValueVector*> flatKeyVectors;
    std::vector<common::ValueVector*> unFlatKeyVectors;
    std::vector<common::ValueVector*> dependentKeyVectors;
    common::DataChunkState* leadingState;

    std::shared_ptr<HashAggregateSharedState> sharedState;
    std::unique_ptr<AggregateHashTable> localAggregateHashTable;
};

} // namespace processor
} // namespace kuzu
