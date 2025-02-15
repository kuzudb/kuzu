#include "processor/operator/aggregate/hash_aggregate.h"

#include <memory>

#include "binder/expression/expression_util.h"
#include "common/assert.h"
#include "common/types/types.h"
#include "main/client_context.h"
#include "processor/execution_context.h"
#include "processor/operator/aggregate/aggregate_hash_table.h"
#include "processor/operator/aggregate/aggregate_input.h"
#include "processor/result/factorized_table_schema.h"

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
HashAggregateSharedState::HashAggregateSharedState(main::ClientContext* context,
    HashAggregateInfo hashAggInfo,
    const std::vector<function::AggregateFunction>& aggregateFunctions,
    std::span<AggregateInfo> aggregateInfos, std::vector<LogicalType> keyTypes,
    std::vector<LogicalType> payloadTypes)
    : BaseAggregateSharedState{aggregateFunctions}, aggInfo{std::move(hashAggInfo)},
      globalPartitions{static_cast<size_t>(context->getMaxNumThreadForExec())},
      limitNumber{common::INVALID_LIMIT}, memoryManager{context->getMemoryManager()},
      // .size() - 1 since we want the bit width of the largest value that could be used to index
      // the partitions
      shiftForPartitioning{
          static_cast<uint8_t>(sizeof(hash_t) * 8 - std::bit_width(globalPartitions.size() - 1))} {
    std::vector<LogicalType> distinctAggregateKeyTypes;
    for (auto& aggInfo : aggregateInfos) {
        distinctAggregateKeyTypes.push_back(aggInfo.distinctAggKeyType.copy());
    }

    // When copying directly into factorizedTables the table's schema's internal mayContainNulls
    // won't be updated and it's probably less work to just always check nulls
    // Skip the last column, which is the hash column and should never contain nulls
    for (size_t i = 0; i < this->aggInfo.tableSchema.getNumColumns() - 1; i++) {
        this->aggInfo.tableSchema.setMayContainsNullsToTrue(i);
    }

    auto& partition = globalPartitions[0];
    partition.queue = std::make_unique<HashTableQueue>(context->getMemoryManager(),
        this->aggInfo.tableSchema.copy());

    // Always create a hash table for the first partition. Any other partitions which are non-empty
    // when finalizing will create an empty copy of this table
    partition.hashTable = std::make_unique<AggregateHashTable>(*context->getMemoryManager(),
        std::move(keyTypes), std::move(payloadTypes), aggregateFunctions, distinctAggregateKeyTypes,
        0, this->aggInfo.tableSchema.copy());
    for (size_t functionIdx = 0; functionIdx < aggregateFunctions.size(); functionIdx++) {
        auto& function = aggregateFunctions[functionIdx];
        if (function.isFunctionDistinct()) {
            // Create table schema for distinct hash table
            auto distinctTableSchema = FactorizedTableSchema();
            // Group by key columns
            for (size_t i = 0;
                 i < this->aggInfo.flatKeysPos.size() + this->aggInfo.unFlatKeysPos.size(); i++) {
                distinctTableSchema.appendColumn(this->aggInfo.tableSchema.getColumn(i)->copy());
                distinctTableSchema.setMayContainsNullsToTrue(i);
            }
            // Distinct key column
            distinctTableSchema.appendColumn(ColumnSchema(false /*isUnFlat*/, 0 /*groupID*/,
                LogicalTypeUtils::getRowLayoutSize(
                    aggregateInfos[functionIdx].distinctAggKeyType)));
            distinctTableSchema.setMayContainsNullsToTrue(distinctTableSchema.getNumColumns() - 1);
            // Hash column
            distinctTableSchema.appendColumn(
                ColumnSchema(false /* isUnFlat */, 0 /* groupID */, sizeof(hash_t)));

            partition.distinctTableQueues.emplace_back(std::make_unique<HashTableQueue>(
                context->getMemoryManager(), std::move(distinctTableSchema)));
        } else {
            // dummy entry so that indices line up with the aggregateFunctions
            partition.distinctTableQueues.emplace_back();
        }
    }
    // Each partition is the same, so we create the list of distinct queues for the first partition
    // and copy it to the other partitions
    for (size_t i = 1; i < globalPartitions.size(); i++) {
        globalPartitions[i].queue = std::make_unique<HashTableQueue>(context->getMemoryManager(),
            this->aggInfo.tableSchema.copy());
        globalPartitions[i].distinctTableQueues.resize(partition.distinctTableQueues.size());
        std::transform(partition.distinctTableQueues.begin(), partition.distinctTableQueues.end(),
            globalPartitions[i].distinctTableQueues.begin(), [&](auto& q) {
                if (q.get() != nullptr) {
                    return q->copy();
                } else {
                    return std::unique_ptr<HashTableQueue>();
                }
            });
    }
}

HashAggregateSharedState::HashTableQueue::HashTableQueue(storage::MemoryManager* memoryManager,
    FactorizedTableSchema tableSchema) {
    headBlock = new TupleBlock(memoryManager, std::move(tableSchema));
}

HashAggregateSharedState::HashTableQueue::~HashTableQueue() {
    delete headBlock.load();
    TupleBlock* block = nullptr;
    while (queuedTuples.pop(block)) {
        delete block;
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
    main::ClientContext* context, std::vector<function::AggregateFunction>& aggregateFunctions,
    std::vector<common::LogicalType> distinctKeyTypes) {
    auto& info = sharedState->getAggregateInfo();
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

    aggregateHashTable = std::make_unique<PartitioningAggregateHashTable>(sharedState,
        *context->getMemoryManager(), std::move(keyDataTypes), std::move(payloadDataTypes),
        aggregateFunctions, std::move(distinctKeyTypes), info.tableSchema.copy());
}

uint64_t HashAggregateLocalState::append(const std::vector<AggregateInput>& aggregateInputs,
    uint64_t multiplicity) const {
    return aggregateHashTable->append(flatKeyVectors, unFlatKeyVectors, dependentKeyVectors,
        leadingState, aggregateInputs, multiplicity);
}

void HashAggregate::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    BaseAggregate::initLocalStateInternal(resultSet, context);
    std::vector<LogicalType> distinctAggKeyTypes;
    for (auto& info : aggInfos) {
        distinctAggKeyTypes.push_back(info.distinctAggKeyType.copy());
    }
    localState.init(sharedState.get(), *resultSet, context->clientContext, aggregateFunctions,
        std::move(distinctAggKeyTypes));
}

void HashAggregate::executeInternal(ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
        const auto numAppendedFlatTuples = localState.append(aggInputs, resultSet->multiplicity);
        metrics->numOutputTuple.increase(numAppendedFlatTuples);
        // Note: The limit count check here is only applicable to the distinct limit case.
        if (localState.aggregateHashTable->getNumEntries() >= sharedState->getLimitNumber()) {
            break;
        }
    }
    localState.aggregateHashTable->mergeAll();
}

uint64_t HashAggregateSharedState::getNumTuples() const {
    uint64_t numTuples = 0;
    for (auto& partition : globalPartitions) {
        numTuples += partition.hashTable->getNumEntries();
    }
    return numTuples;
}

void HashAggregateSharedState::HashTableQueue::appendTuple(std::span<uint8_t> tuple) {
    while (true) {
        auto* block = headBlock.load();
        KU_ASSERT(tuple.size() == block->table.getTableSchema()->getNumBytesPerTuple());
        auto posToWrite = block->numTuplesReserved++;
        if (posToWrite < block->table.getNumTuplesPerBlock()) {
            memcpy(block->table.getTuple(posToWrite), tuple.data(), tuple.size());
            block->numTuplesWritten++;
            return;
        } else {
            // No more space in the block, allocate and replace it
            auto* newBlock = new TupleBlock(block->table.getMemoryManager(),
                block->table.getTableSchema()->copy());
            if (headBlock.compare_exchange_strong(block, newBlock)) {
                // TODO(bmwinger): if the queuedTuples has at least a certain size (benchmark to see
                // if there's a benefit to waiting for multiple blocks) then cycle through the queue
                // and flush any blocks which have been fully written
                queuedTuples.push(block);
            } else {
                // If the block was replaced by another thread, discard the block we created and try
                // again with the block allocated by the other thread
                delete newBlock;
            }
        }
    }
}

void HashAggregateSharedState::HashTableQueue::mergeInto(AggregateHashTable& hashTable) {
    std::vector<TupleBlock*> partitionsToMerge;
    partitionsToMerge.reserve(queuedTuples.approxSize());
    TupleBlock* partitionToMerge = nullptr;
    // Over-estimate the total number of distinct groups using the total number of tuples
    // after the per-thread pre-aggregation
    // This will probably use more memory than necessary, but will greatly reduce the need
    // to resize the hash table. It will only use memory proportional to what is already
    // used by the queuedTuples already, and the hash slots are usually small in comparison
    // to the full tuple data.
    auto headBlock = this->headBlock.load();
    KU_ASSERT(headBlock != nullptr);
    hashTable.resizeHashTableIfNecessary(
        queuedTuples.approxSize() * headBlock->table.getNumTuplesPerBlock() +
        headBlock->numTuplesWritten);
    while (queuedTuples.pop(partitionToMerge)) {
        KU_ASSERT(
            partitionToMerge->numTuplesWritten == partitionToMerge->table.getNumTuplesPerBlock());
        hashTable.merge(std::move(partitionToMerge->table));
        delete partitionToMerge;
    }
    if (headBlock->numTuplesWritten > 0) {
        headBlock->table.resize(headBlock->numTuplesWritten);
        hashTable.merge(std::move(headBlock->table));
    }
    delete headBlock;
    this->headBlock = nullptr;
}

void HashAggregateSharedState::finalizeAggregateHashTable() {
    for (auto& partition : globalPartitions) {
        if (!partition.finalized && partition.mtx.try_lock()) {
            if (partition.finalized) {
                // If there was a data race in the above && a thread may get through after another
                // thread has finalized this partition
                // TODO(bmwinger): While it's not currently necessary to unlock the mutexes here,
                // we can use the locks again to start scanning before all partitions are finalized.
                partition.mtx.unlock();
                continue;
            }
            if (!partition.hashTable) {
                // We always initialize the hash table in the first partition
                partition.hashTable = std::make_unique<AggregateHashTable>(
                    globalPartitions[0].hashTable->createEmptyCopy());
            }
            // TODO(bmwinger): ideally these can be merged into a single function.
            // The distinct tables need to be merged first so that they exist when the other table
            // updates the agg states when it merges
            for (size_t i = 0; i < partition.distinctTableQueues.size(); i++) {
                if (partition.distinctTableQueues[i]) {
                    partition.distinctTableQueues[i]->mergeInto(
                        *partition.hashTable->getDistinctHashTable(i));
                }
            }
            partition.queue->mergeInto(*partition.hashTable);
            partition.hashTable->mergeDistinctAggregateInfo();

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
    const auto* table = globalPartitions[partitionIdx].hashTable->getFactorizedTable();
    while (factorizedTableStartOffset + table->getNumTuples() <= offset) {
        factorizedTableStartOffset += table->getNumTuples();
        table = globalPartitions[++partitionIdx].hashTable->getFactorizedTable();
    }
    return std::make_tuple(table, factorizedTableStartOffset);
}

void HashAggregateSharedState::scan(std::span<uint8_t*> entries,
    std::vector<common::ValueVector*>& keyVectors, offset_t startOffset, offset_t numTuplesToScan,
    std::vector<uint32_t>& columnIndices) {
    auto [table, tableStartOffset] = getPartitionForOffset(startOffset);
    // Due to the way FactorizedTable::lookup works, it's necessary to read one partition
    // at a time.
    KU_ASSERT(startOffset - tableStartOffset + numTuplesToScan <= table->getNumTuples());
    for (size_t pos = 0; pos < numTuplesToScan; pos++) {
        auto posInTable = startOffset + pos - tableStartOffset;
        entries[pos] = table->getTuple(posInTable);
    }
    table->lookup(keyVectors, columnIndices, entries.data(), 0, numTuplesToScan);
    KU_ASSERT(true);
}

void HashAggregateSharedState::assertFinalized() const {
    RUNTIME_CHECK(for (const auto& partition
                       : globalPartitions) {
        KU_ASSERT(partition.finalized);
        KU_ASSERT(partition.queue->empty());
    });
}

} // namespace processor
} // namespace kuzu
