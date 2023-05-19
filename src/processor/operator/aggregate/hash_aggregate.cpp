#include "processor/operator/aggregate/hash_aggregate.h"

using namespace kuzu::common;
using namespace kuzu::function;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void HashAggregateSharedState::appendAggregateHashTable(
    std::unique_ptr<AggregateHashTable> aggregateHashTable) {
    std::unique_lock lck{mtx};
    localAggregateHashTables.push_back(std::move(aggregateHashTable));
}

void HashAggregateSharedState::combineAggregateHashTable(MemoryManager& memoryManager) {
    std::unique_lock lck{mtx};
    if (localAggregateHashTables.size() == 1) {
        globalAggregateHashTable = std::move(localAggregateHashTables[0]);
    } else {
        auto numEntries = 0u;
        for (auto& ht : localAggregateHashTables) {
            numEntries += ht->getNumEntries();
        }
        localAggregateHashTables[0]->resize(nextPowerOfTwo(numEntries));
        globalAggregateHashTable = std::move(localAggregateHashTables[0]);
        for (auto i = 1u; i < localAggregateHashTables.size(); i++) {
            globalAggregateHashTable->merge(*localAggregateHashTables[i]);
        }
    }
}

void HashAggregateSharedState::finalizeAggregateHashTable() {
    std::unique_lock lck{mtx};
    globalAggregateHashTable->finalizeAggregateStates();
}

std::pair<uint64_t, uint64_t> HashAggregateSharedState::getNextRangeToRead() {
    std::unique_lock lck{mtx};
    if (currentOffset >= globalAggregateHashTable->getNumEntries()) {
        return std::make_pair(currentOffset, currentOffset);
    }
    auto startOffset = currentOffset;
    auto range = std::min(
        DEFAULT_VECTOR_CAPACITY, globalAggregateHashTable->getNumEntries() - currentOffset);
    currentOffset += range;
    return std::make_pair(startOffset, startOffset + range);
}

void HashAggregate::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    BaseAggregate::initLocalStateInternal(resultSet, context);
    std::vector<LogicalType> keyDataTypes;
    for (auto& pos : flatKeysPos) {
        auto vector = resultSet->getValueVector(pos).get();
        flatKeyVectors.push_back(vector);
        keyDataTypes.push_back(vector->dataType);
    }
    for (auto& pos : unFlatKeysPos) {
        auto vector = resultSet->getValueVector(pos).get();
        unFlatKeyVectors.push_back(vector);
        keyDataTypes.push_back(vector->dataType);
    }
    std::vector<LogicalType> payloadDataTypes;
    for (auto& pos : dependentKeysPos) {
        auto vector = resultSet->getValueVector(pos).get();
        dependentKeyVectors.push_back(vector);
        payloadDataTypes.push_back(vector->dataType);
    }
    localAggregateHashTable = make_unique<AggregateHashTable>(
        *context->memoryManager, keyDataTypes, payloadDataTypes, aggregateFunctions, 0);
}

void HashAggregate::executeInternal(ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
        localAggregateHashTable->append(flatKeyVectors, unFlatKeyVectors, dependentKeyVectors,
            aggregateInputs, resultSet->multiplicity);
    }
    sharedState->appendAggregateHashTable(std::move(localAggregateHashTable));
}

void HashAggregate::finalize(ExecutionContext* context) {
    sharedState->combineAggregateHashTable(*context->memoryManager);
    sharedState->finalizeAggregateHashTable();
}

} // namespace processor
} // namespace kuzu
