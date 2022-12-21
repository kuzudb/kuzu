#pragma once

#include "aggregate_hash_table.h"
#include "processor/operator/aggregate/base_aggregate.h"

namespace kuzu {
namespace processor {

class HashAggregateSharedState : public BaseAggregateSharedState {

public:
    explicit HashAggregateSharedState(
        const vector<unique_ptr<AggregateFunction>>& aggregateFunctions)
        : BaseAggregateSharedState{aggregateFunctions} {}

    void appendAggregateHashTable(unique_ptr<AggregateHashTable> aggregateHashTable);

    void combineAggregateHashTable(MemoryManager& memoryManager);

    void finalizeAggregateHashTable();

    pair<uint64_t, uint64_t> getNextRangeToRead() override;

    inline uint8_t* getRow(uint64_t idx) { return globalAggregateHashTable->getEntry(idx); }

    FactorizedTable* getFactorizedTable() { return globalAggregateHashTable->getFactorizedTable(); }

private:
    vector<unique_ptr<AggregateHashTable>> localAggregateHashTables;
    unique_ptr<AggregateHashTable> globalAggregateHashTable;
};

class HashAggregate : public BaseAggregate {
public:
    HashAggregate(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        shared_ptr<HashAggregateSharedState> sharedState,
        vector<DataPos> inputGroupByHashKeyVectorsPos,
        vector<DataPos> inputGroupByNonHashKeyVectorsPos,
        vector<bool> isInputGroupByHashKeyVectorFlat, vector<DataPos> aggregateVectorsPos,
        vector<unique_ptr<AggregateFunction>> aggregateFunctions,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : BaseAggregate{std::move(resultSetDescriptor), std::move(aggregateVectorsPos),
              std::move(aggregateFunctions), std::move(child), id, paramsString},
          groupByHashKeyVectorsPos{std::move(inputGroupByHashKeyVectorsPos)},
          groupByNonHashKeyVectorsPos{std::move(inputGroupByNonHashKeyVectorsPos)},
          isGroupByHashKeyVectorFlat{std::move(isInputGroupByHashKeyVectorFlat)},
          sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    void finalize(ExecutionContext* context) override;

    unique_ptr<PhysicalOperator> clone() override;

private:
    vector<DataPos> groupByHashKeyVectorsPos;
    vector<DataPos> groupByNonHashKeyVectorsPos;
    vector<bool> isGroupByHashKeyVectorFlat;
    vector<ValueVector*> groupByFlatHashKeyVectors;
    vector<ValueVector*> groupByUnflatHashKeyVectors;
    vector<ValueVector*> groupByNonHashKeyVectors;

    shared_ptr<HashAggregateSharedState> sharedState;
    unique_ptr<AggregateHashTable> localAggregateHashTable;
};

} // namespace processor
} // namespace kuzu
