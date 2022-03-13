#include "src/processor/include/physical_plan/operator/aggregate/hash_aggregate.h"

namespace graphflow {
namespace processor {

void HashAggregateSharedState::appendAggregateHashTable(
    unique_ptr<AggregateHashTable> aggregateHashTable) {
    auto lck = acquireLock();
    localAggregateHashTables.push_back(move(aggregateHashTable));
}

void HashAggregateSharedState::combineAggregateHashTable(MemoryManager& memoryManager) {
    auto lck = acquireLock();
    auto numEntries = 0u;
    for (auto& ht : localAggregateHashTables) {
        numEntries += ht->getNumEntries();
    }
    globalAggregateHashTable = make_unique<AggregateHashTable>(memoryManager,
        localAggregateHashTables[0]->getGroupByHashKeysDataTypes(),
        localAggregateHashTables[0]->getGroupByNonHashKeysDataTypes(), aggregateFunctions,
        numEntries);
    for (auto& ht : localAggregateHashTables) {
        globalAggregateHashTable->merge(*ht);
    }
}

void HashAggregateSharedState::finalizeAggregateHashTable() {
    auto lck = acquireLock();
    globalAggregateHashTable->finalizeAggregateStates();
}

pair<uint64_t, uint64_t> HashAggregateSharedState::getNextRangeToRead() {
    auto lck = acquireLock();
    if (currentOffset >= globalAggregateHashTable->getNumEntries()) {
        return make_pair(currentOffset, currentOffset);
    }
    auto startOffset = currentOffset;
    auto range =
        min(DEFAULT_VECTOR_CAPACITY, globalAggregateHashTable->getNumEntries() - currentOffset);
    currentOffset += range;
    return make_pair(startOffset, startOffset + range);
}

HashAggregate::HashAggregate(shared_ptr<HashAggregateSharedState> sharedState,
    vector<DataPos> inputGroupByHashKeyVectorsPos, vector<DataPos> inputGroupByNonHashKeyVectorsPos,
    vector<DataPos> aggregateVectorsPos, vector<unique_ptr<AggregateFunction>> aggregateFunctions,
    unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
    : BaseAggregate{move(aggregateVectorsPos), move(aggregateFunctions), move(child), context, id},
      groupByHashKeyVectorsPos{move(inputGroupByHashKeyVectorsPos)},
      groupByNonHashKeyVectorsPos{move(inputGroupByNonHashKeyVectorsPos)}, sharedState{
                                                                               move(sharedState)} {}

shared_ptr<ResultSet> HashAggregate::initResultSet() {
    resultSet = BaseAggregate::initResultSet();
    vector<DataType> groupByHashKeysDataTypes;
    for (auto& dataPos : groupByHashKeyVectorsPos) {
        auto dataChunk = resultSet->dataChunks[dataPos.dataChunkPos];
        auto vector = dataChunk->valueVectors[dataPos.valueVectorPos].get();
        groupByHashKeyVectors.push_back(vector);
        groupByHashKeysDataTypes.push_back(vector->dataType);
    }
    vector<DataType> groupByNonHashKeysDataTypes;
    for (auto& dataPos : groupByNonHashKeyVectorsPos) {
        auto dataChunk = resultSet->dataChunks[dataPos.dataChunkPos];
        auto vector = dataChunk->valueVectors[dataPos.valueVectorPos].get();
        groupByNonHashKeyVectors.push_back(vector);
        groupByNonHashKeysDataTypes.push_back(vector->dataType);
    }
    localAggregateHashTable = make_unique<AggregateHashTable>(*context.memoryManager,
        groupByHashKeysDataTypes, groupByNonHashKeysDataTypes, aggregateFunctions, 0);
    return resultSet;
}

void HashAggregate::execute() {
    metrics->executionTime.start();
    BaseAggregate::execute();
    while (children[0]->getNextTuples()) {
        localAggregateHashTable->append(groupByHashKeyVectors, groupByNonHashKeyVectors,
            aggregateVectors, resultSet->multiplicity);
    }
    sharedState->appendAggregateHashTable(move(localAggregateHashTable));
    metrics->executionTime.stop();
}

void HashAggregate::finalize() {
    sharedState->combineAggregateHashTable(*context.memoryManager);
    sharedState->finalizeAggregateHashTable();
}

unique_ptr<PhysicalOperator> HashAggregate::clone() {
    vector<unique_ptr<AggregateFunction>> clonedAggregateFunctions;
    for (auto& aggregateFunction : aggregateFunctions) {
        clonedAggregateFunctions.push_back(aggregateFunction->clone());
    }
    return make_unique<HashAggregate>(sharedState, groupByHashKeyVectorsPos,
        groupByNonHashKeyVectorsPos, aggregateVectorsPos, move(clonedAggregateFunctions),
        children[0]->clone(), context, id);
}

} // namespace processor
} // namespace graphflow
