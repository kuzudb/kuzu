#include "processor/operator/aggregate/hash_aggregate.h"

namespace kuzu {
namespace processor {

void HashAggregateSharedState::appendAggregateHashTable(
    unique_ptr<AggregateHashTable> aggregateHashTable) {
    auto lck = acquireLock();
    localAggregateHashTables.push_back(move(aggregateHashTable));
}

void HashAggregateSharedState::combineAggregateHashTable(MemoryManager& memoryManager) {
    auto lck = acquireLock();
    if (localAggregateHashTables.size() == 1) {
        globalAggregateHashTable = move(localAggregateHashTables[0]);
    } else {
        auto numEntries = 0u;
        for (auto& ht : localAggregateHashTables) {
            numEntries += ht->getNumEntries();
        }
        localAggregateHashTables[0]->resize(HashTableUtils::nextPowerOfTwo(numEntries));
        globalAggregateHashTable = move(localAggregateHashTables[0]);
        for (auto i = 1u; i < localAggregateHashTables.size(); i++) {
            globalAggregateHashTable->merge(*localAggregateHashTables[i]);
        }
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

void HashAggregate::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    BaseAggregate::initLocalStateInternal(resultSet, context);
    vector<DataType> groupByHashKeysDataTypes;
    for (auto i = 0u; i < groupByHashKeyVectorsPos.size(); i++) {
        auto vector = resultSet->getValueVector(groupByHashKeyVectorsPos[i]).get();
        if (isGroupByHashKeyVectorFlat[i]) {
            groupByFlatHashKeyVectors.push_back(vector);
        } else {
            groupByUnflatHashKeyVectors.push_back(vector);
        }
        groupByHashKeysDataTypes.push_back(vector->dataType);
    }
    vector<DataType> groupByNonHashKeysDataTypes;
    for (auto& dataPos : groupByNonHashKeyVectorsPos) {
        auto vector = resultSet->getValueVector(dataPos).get();
        groupByNonHashKeyVectors.push_back(vector);
        groupByNonHashKeysDataTypes.push_back(vector->dataType);
    }
    localAggregateHashTable = make_unique<AggregateHashTable>(*context->memoryManager,
        groupByHashKeysDataTypes, groupByNonHashKeysDataTypes, aggregateFunctions, 0);
}

void HashAggregate::executeInternal(ExecutionContext* context) {
    while (children[0]->getNextTuple()) {
        localAggregateHashTable->append(groupByFlatHashKeyVectors, groupByUnflatHashKeyVectors,
            groupByNonHashKeyVectors, aggregateVectors, resultSet->multiplicity);
    }
    sharedState->appendAggregateHashTable(move(localAggregateHashTable));
}

void HashAggregate::finalize(ExecutionContext* context) {
    sharedState->combineAggregateHashTable(*context->memoryManager);
    sharedState->finalizeAggregateHashTable();
}

unique_ptr<PhysicalOperator> HashAggregate::clone() {
    vector<unique_ptr<AggregateFunction>> clonedAggregateFunctions;
    for (auto& aggregateFunction : aggregateFunctions) {
        clonedAggregateFunctions.push_back(aggregateFunction->clone());
    }
    return make_unique<HashAggregate>(resultSetDescriptor->copy(), sharedState,
        groupByHashKeyVectorsPos, groupByNonHashKeyVectorsPos, isGroupByHashKeyVectorFlat,
        aggregateVectorsPos, std::move(clonedAggregateFunctions), children[0]->clone(), id,
        paramsString);
}

} // namespace processor
} // namespace kuzu
