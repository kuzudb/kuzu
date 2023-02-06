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
        std::shared_ptr<HashAggregateSharedState> sharedState,
        std::vector<DataPos> inputGroupByHashKeyVectorsPos,
        std::vector<DataPos> inputGroupByNonHashKeyVectorsPos,
        std::vector<bool> isInputGroupByHashKeyVectorFlat, std::vector<DataPos> aggregateVectorsPos,
        std::vector<std::unique_ptr<function::AggregateFunction>> aggregateFunctions,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : BaseAggregate{std::move(resultSetDescriptor), std::move(aggregateVectorsPos),
              std::move(aggregateFunctions), std::move(child), id, paramsString},
          groupByHashKeyVectorsPos{std::move(inputGroupByHashKeyVectorsPos)},
          groupByNonHashKeyVectorsPos{std::move(inputGroupByNonHashKeyVectorsPos)},
          isGroupByHashKeyVectorFlat{std::move(isInputGroupByHashKeyVectorFlat)},
          sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    void finalize(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override;

private:
    std::vector<DataPos> groupByHashKeyVectorsPos;
    std::vector<DataPos> groupByNonHashKeyVectorsPos;
    std::vector<bool> isGroupByHashKeyVectorFlat;
    std::vector<common::ValueVector*> groupByFlatHashKeyVectors;
    std::vector<common::ValueVector*> groupByUnflatHashKeyVectors;
    std::vector<common::ValueVector*> groupByNonHashKeyVectors;

    std::shared_ptr<HashAggregateSharedState> sharedState;
    std::unique_ptr<AggregateHashTable> localAggregateHashTable;
};

} // namespace processor
} // namespace kuzu
