#pragma once

#include "src/processor/include/physical_plan/hash_table/aggregate_hash_table.h"
#include "src/processor/include/physical_plan/operator/aggregate/base_aggregate.h"

namespace graphflow {
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
    HashAggregate(shared_ptr<HashAggregateSharedState> sharedState,
        vector<DataPos> groupByKeyVectorsPos, vector<DataPos> aggregateVectorsPos,
        vector<unique_ptr<AggregateFunction>> aggregateFunctions,
        unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id);

    shared_ptr<ResultSet> initResultSet() override;

    void execute() override;

    void finalize() override;

    unique_ptr<PhysicalOperator> clone() override;

private:
    vector<DataPos> groupByKeyVectorsPos;
    vector<ValueVector*> groupByKeyVectors;

    shared_ptr<HashAggregateSharedState> sharedState;
    unique_ptr<AggregateHashTable> localAggregateHashTable;
};

} // namespace processor
} // namespace graphflow
