#include "processor/operator/aggregate/hash_aggregate.h"

#include <thread>

#include "binder/expression/expression_util.h"
#include "common/assert.h"
#include "common/constants.h"
#include "common/types/types.h"
#include "common/utils.h"
#include "main/client_context.h"
#include "processor/operator/aggregate/aggregate_hash_table.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::common;
using namespace kuzu::function;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::string HashAggregatePrintInfo::toString() const {
    std::string result = "";
    result += "Group By: ";
    result += binder::ExpressionUtil::toString(keys);
    if (!aggregates.empty()) {
        result += ", Aggregates: ";
        result += binder::ExpressionUtil::toString(aggregates);
    }
    if (limitNum != UINT64_MAX) {
        result += ", Distinct Limit: " + std::to_string(limitNum);
    }
    return result;
}
HashAggregateSharedState::HashAggregateSharedState(
    const std::vector<function::AggregateFunction>& aggregateFunctions)
    : BaseAggregateSharedState{aggregateFunctions}, limitCounter{0},
      limitNumber{common::INVALID_LIMIT}, numThreads{0} {}

void HashAggregateSharedState::initPartitions(main::ClientContext* context,
    std::vector<LogicalType> keyDataTypes, std::vector<LogicalType> payloadDataTypes,
    HashAggregateInfo& info, std::vector<common::LogicalType> types) {
    for (auto& partition : globalPartitions) {
        if (!partition.hashTable && partition.mtx.try_lock()) {
            partition.hashTable = std::make_unique<AggregateHashTable>(*context->getMemoryManager(),
                LogicalType::copy(keyDataTypes), LogicalType::copy(payloadDataTypes),
                aggregateFunctions, types, 0, info.tableSchema.copy());
            partition.mtx.unlock();
        }
    }
}

bool HashAggregateSharedState::increaseAndCheckLimitCount(uint64_t num) {
    if (limitNumber == common::INVALID_LIMIT) {
        return false;
    } else {
        return limitCounter.fetch_add(num) >= limitNumber;
    }
}

std::pair<uint64_t, uint64_t> HashAggregateSharedState::getNextRangeToRead() {
    std::unique_lock lck{mtx};
    auto startOffset = currentOffset;
    auto numTuples = getNumTuples();
    if (startOffset >= numTuples) {
        return std::make_pair(startOffset, startOffset);
    }
    // FactorizedTable::lookup resets the ValueVector and writes to the beginning,
    // so we can't support scanning from multiple partitions at once
    auto [table, tableStartOffset] = getPartitionForOffset(startOffset);
    auto range = std::min(std::min(DEFAULT_VECTOR_CAPACITY, numTuples - startOffset),
        table->getNumTuples() + tableStartOffset - startOffset);
    currentOffset += range;
    return std::make_pair(startOffset, startOffset + range);
}

HashAggregateInfo::HashAggregateInfo(std::vector<DataPos> flatKeysPos,
    std::vector<DataPos> unFlatKeysPos, std::vector<DataPos> dependentKeysPos,
    FactorizedTableSchema tableSchema)
    : flatKeysPos{std::move(flatKeysPos)}, unFlatKeysPos{std::move(unFlatKeysPos)},
      dependentKeysPos{std::move(dependentKeysPos)}, tableSchema{std::move(tableSchema)} {}

HashAggregateInfo::HashAggregateInfo(const HashAggregateInfo& other)
    : flatKeysPos{other.flatKeysPos}, unFlatKeysPos{other.unFlatKeysPos},
      dependentKeysPos{other.dependentKeysPos}, tableSchema{other.tableSchema.copy()} {}

void HashAggregateLocalState::init(HashAggregateSharedState* sharedState, ResultSet& resultSet,
    main::ClientContext* context, HashAggregateInfo& info,
    std::vector<function::AggregateFunction>& aggregateFunctions,
    std::vector<common::LogicalType> types) {
    std::vector<LogicalType> keyDataTypes;
    for (auto& pos : info.flatKeysPos) {
        auto vector = resultSet.getValueVector(pos).get();
        flatKeyVectors.push_back(vector);
        keyDataTypes.push_back(vector->dataType.copy());
    }
    for (auto& pos : info.unFlatKeysPos) {
        auto vector = resultSet.getValueVector(pos).get();
        unFlatKeyVectors.push_back(vector);
        keyDataTypes.push_back(vector->dataType.copy());
    }
    std::vector<LogicalType> payloadDataTypes;
    for (auto& pos : info.dependentKeysPos) {
        auto vector = resultSet.getValueVector(pos).get();
        dependentKeyVectors.push_back(vector);
        payloadDataTypes.push_back(vector->dataType.copy());
    }
    leadingState = unFlatKeyVectors.empty() ? flatKeyVectors[0]->state.get() :
                                              unFlatKeyVectors[0]->state.get();

    sharedState->initPartitions(context, LogicalType::copy(keyDataTypes),
        LogicalType::copy(payloadDataTypes), info, LogicalType::copy(types));
    sharedState->registerThread();

    aggregateHashTable = std::make_unique<PartitioningAggregateHashTable>(sharedState,
        *context->getMemoryManager(), std::move(keyDataTypes), std::move(payloadDataTypes),
        aggregateFunctions, std::move(types), std::move(info.tableSchema));
}

uint64_t HashAggregateLocalState::append(const std::vector<AggregateInput>& aggregateInputs,
    uint64_t multiplicity) {
    return aggregateHashTable->append(flatKeyVectors, unFlatKeyVectors, dependentKeyVectors,
        leadingState, aggregateInputs, multiplicity);
}

void HashAggregate::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    BaseAggregate::initLocalStateInternal(resultSet, context);
    std::vector<LogicalType> distinctAggKeyTypes;
    for (auto& info : aggInfos) {
        distinctAggKeyTypes.push_back(info.distinctAggKeyType.copy());
    }
    localState.init(sharedState.get(), *resultSet, context->clientContext, hashInfo,
        aggregateFunctions, std::move(distinctAggKeyTypes));
}

void HashAggregate::executeInternal(ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
        const auto numAppendedFlatTuples = localState.append(aggInputs, resultSet->multiplicity);
        metrics->numOutputTuple.increase(numAppendedFlatTuples);
        // Note: The limit count check here is only applicable to the distinct limit case.
        if (sharedState->increaseAndCheckLimitCount(numAppendedFlatTuples)) {
            break;
        }
    }
    localState.aggregateHashTable->mergeAll();
    sharedState->setThreadFinishedProducing();
    while (!sharedState->allThreadsFinishedProducing()) {
        std::this_thread::sleep_for(std::chrono::microseconds(500));
    }
    sharedState->finalizeAggregateHashTable();
}

void HashAggregate::finalizeInternal(ExecutionContext*) {
    sharedState->assertFinalized();
}

uint64_t HashAggregateSharedState::getNumTuples() const {
    uint64_t numTuples = 0;
    for (auto& partition : globalPartitions) {
        numTuples += partition.hashTable->getNumEntries();
    }
    return numTuples;
}

std::unique_ptr<FactorizedTable> HashAggregateSharedState::mergeTable(uint8_t partitionIdx,
    std::unique_ptr<FactorizedTable> localPartition) {
    auto& partition = globalPartitions[partitionIdx];
    if (partition.mtx.try_lock()) {
        partition.hashTable->merge(*localPartition);
        // TODO(bmwinger): clear does allocations. This should ideally re-use the existing data
        // structures
        localPartition->clear();
        std::unique_ptr<FactorizedTable> partitionToMerge;
        while (partition.queuedTuples.pop(partitionToMerge)) {
            partition.hashTable->merge(*partitionToMerge);
        }
        partition.mtx.unlock();
    } else {
        partition.queuedTuples.push(std::move(localPartition));
        localPartition = nullptr;
    }
    return localPartition;
}

void HashAggregateSharedState::finalizeAggregateHashTable() {
    for (auto& partition : globalPartitions) {
        if (partition.mtx.try_lock()) {
            std::unique_ptr<FactorizedTable> partitionToMerge;
            while (partition.queuedTuples.pop(partitionToMerge)) {
                partition.hashTable->merge(*partitionToMerge);
            }
            partition.hashTable->finalizeAggregateStates();
            partition.finalized = true;
            partition.mtx.unlock();
        }
    }
}

std::tuple<const FactorizedTable*, offset_t> HashAggregateSharedState::getPartitionForOffset(
    offset_t offset) const {
    auto factorizedTableStartOffset = 0;
    auto partitionIdx = 0;
    auto* factorizedTable = globalPartitions[partitionIdx].hashTable->getFactorizedTable();
    while (factorizedTableStartOffset + factorizedTable->getNumTuples() <= offset) {
        factorizedTableStartOffset += factorizedTable->getNumTuples();
        factorizedTable = globalPartitions[++partitionIdx].hashTable->getFactorizedTable();
    }
    return std::make_tuple(factorizedTable, factorizedTableStartOffset);
}

void HashAggregateSharedState::scan(std::span<uint8_t*> entries,
    std::vector<common::ValueVector*>& keyVectors, offset_t startOffset, offset_t numTuplesToScan,
    std::vector<uint32_t>& columnIndices) {
    auto [factorizedTable, factorizedTableStartOffset] = getPartitionForOffset(startOffset);
    // Due to the way FactorizedTable::lookup works, it's necessary to read one partition
    // at a time.
    KU_ASSERT(startOffset - factorizedTableStartOffset + numTuplesToScan <=
              factorizedTable->getNumTuples());
    for (size_t pos = 0; pos < numTuplesToScan; pos++) {
        auto posInTable = startOffset + pos - factorizedTableStartOffset;
        entries[pos] = factorizedTable->getTuple(posInTable);
    }
    factorizedTable->lookup(keyVectors, columnIndices, entries.data(), 0, numTuplesToScan);
    KU_ASSERT(true);
}

void HashAggregateSharedState::assertFinalized() const {
    RUNTIME_CHECK(for (const auto& partition
                       : globalPartitions) {
        KU_ASSERT(partition.finalized);
        KU_ASSERT(partition.queuedTuples.approxSize() == 0);
    });
}

} // namespace processor
} // namespace kuzu
