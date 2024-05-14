#include "processor/operator/aggregate/hash_aggregate.h"

#include "common/utils.h"
#include "processor/result/mark_hash_table.h"

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

void HashAggregateSharedState::combineAggregateHashTable(MemoryManager& /*memoryManager*/) {
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
    auto range = std::min(DEFAULT_VECTOR_CAPACITY,
        globalAggregateHashTable->getNumEntries() - currentOffset);
    currentOffset += range;
    return std::make_pair(startOffset, startOffset + range);
}

HashAggregateInfo::HashAggregateInfo(std::vector<DataPos> flatKeysPos,
    std::vector<DataPos> unFlatKeysPos, std::vector<DataPos> dependentKeysPos,
    FactorizedTableSchema tableSchema, HashTableType hashTableType)
    : flatKeysPos{std::move(flatKeysPos)}, unFlatKeysPos{std::move(unFlatKeysPos)},
      dependentKeysPos{std::move(dependentKeysPos)}, tableSchema{std::move(tableSchema)},
      hashTableType{hashTableType} {}

HashAggregateInfo::HashAggregateInfo(const HashAggregateInfo& other)
    : flatKeysPos{other.flatKeysPos}, unFlatKeysPos{other.unFlatKeysPos},
      dependentKeysPos{other.dependentKeysPos}, tableSchema{other.tableSchema.copy()},
      hashTableType{other.hashTableType} {}

void HashAggregateLocalState::init(ResultSet& resultSet, main::ClientContext* context,
    HashAggregateInfo& info,
    std::vector<std::unique_ptr<function::AggregateFunction>>& aggregateFunctions,
    std::vector<common::LogicalType> types) {
    std::vector<LogicalType> keyDataTypes;
    for (auto& pos : info.flatKeysPos) {
        auto vector = resultSet.getValueVector(pos).get();
        flatKeyVectors.push_back(vector);
        keyDataTypes.push_back(vector->dataType);
    }
    for (auto& pos : info.unFlatKeysPos) {
        auto vector = resultSet.getValueVector(pos).get();
        unFlatKeyVectors.push_back(vector);
        keyDataTypes.push_back(vector->dataType);
    }
    std::vector<LogicalType> payloadDataTypes;
    for (auto& pos : info.dependentKeysPos) {
        auto vector = resultSet.getValueVector(pos).get();
        dependentKeyVectors.push_back(vector);
        payloadDataTypes.push_back(vector->dataType);
    }
    leadingState = unFlatKeyVectors.empty() ? flatKeyVectors[0]->state.get() :
                                              unFlatKeyVectors[0]->state.get();
    switch (info.hashTableType) {
    case HashTableType::AGGREGATE_HASH_TABLE:
        aggregateHashTable =
            std::make_unique<AggregateHashTable>(*context->getMemoryManager(), keyDataTypes,
                payloadDataTypes, aggregateFunctions, types, 0, std::move(info.tableSchema));
        break;
    case HashTableType::MARK_HASH_TABLE:
        aggregateHashTable = std::make_unique<MarkHashTable>(*context->getMemoryManager(),
            keyDataTypes, payloadDataTypes, 0, std::move(info.tableSchema));
        break;
    default:
        KU_UNREACHABLE;
    }
}

void HashAggregateLocalState::append(const std::vector<AggregateInput>& aggregateInputs,
    uint64_t multiplicity) const {
    aggregateHashTable->append(flatKeyVectors, unFlatKeyVectors, dependentKeyVectors, leadingState,
        aggregateInputs, multiplicity);
}

void HashAggregate::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    BaseAggregate::initLocalStateInternal(resultSet, context);
    std::vector<LogicalType> distinctAggKeyTypes;
    for (auto& info : aggInfos) {
        distinctAggKeyTypes.push_back(info.distinctAggKeyType);
    }
    localState.init(*resultSet, context->clientContext, hashInfo, aggregateFunctions,
        distinctAggKeyTypes);
}

void HashAggregate::executeInternal(ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
        localState.append(aggInputs, resultSet->multiplicity);
    }
    sharedState->appendAggregateHashTable(std::move(localState.aggregateHashTable));
}

void HashAggregate::finalize(ExecutionContext* context) {
    sharedState->combineAggregateHashTable(*context->clientContext->getMemoryManager());
    sharedState->finalizeAggregateHashTable();
}

} // namespace processor
} // namespace kuzu
