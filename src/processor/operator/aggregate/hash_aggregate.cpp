#include "processor/operator/aggregate/hash_aggregate.h"

#include <chrono>
#include <thread>

#include "binder/expression/expression_util.h"
#include "common/assert.h"
#include "common/constants.h"
#include "common/types/types.h"
#include "main/client_context.h"
#include "processor/execution_context.h"
#include "processor/operator/aggregate/aggregate_hash_table.h"
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
    HashAggregateInfo aggInfo, const std::vector<function::AggregateFunction>& aggregateFunctions)
    : BaseAggregateSharedState{aggregateFunctions}, aggInfo{std::move(aggInfo)},
      globalPartitions{static_cast<size_t>(context->getMaxNumThreadForExec())},
      limitNumber{common::INVALID_LIMIT}, numThreads{0}, memoryManager{context->getMemoryManager()},
      // .size() - 1 since we want the bit width of the largest value that could be used to index
      // the partitions
      shiftForPartitioning{
          static_cast<uint8_t>(sizeof(hash_t) * 8 - std::bit_width(globalPartitions.size() - 1))} {

    // When copying directly into factorizedTables the table's schema's internal mayContainNulls
    // won't be updated and it's probably less work to just always check nulls
    for (size_t i = 0; i < this->aggInfo.tableSchema.getNumColumns(); i++) {
        this->aggInfo.tableSchema.setMayContainsNullsToTrue(i);
    }
    for (auto& partition : globalPartitions) {
        partition.headBlock = new Partition::TupleBlock(context->getMemoryManager(),
            this->aggInfo.tableSchema.copy());
    }
}

HashAggregateSharedState::~HashAggregateSharedState() {
    for (auto& partition : globalPartitions) {
        delete partition.headBlock.load();
        Partition::TupleBlock* block = nullptr;
        while (partition.queuedTuples.pop(block)) {
            delete block;
        }
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
    std::vector<common::LogicalType> types) {
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

    sharedState->registerThread();

    aggregateHashTable = std::make_unique<PartitioningAggregateHashTable>(sharedState,
        *context->getMemoryManager(), std::move(keyDataTypes), std::move(payloadDataTypes),
        aggregateFunctions, std::move(types), info.tableSchema.copy());
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
    sharedState->setThreadFinishedProducing();
    while (!sharedState->allThreadsFinishedProducing()) {
        std::this_thread::sleep_for(std::chrono::microseconds(500));
    }
    sharedState->finalizeAggregateHashTable(*localState.aggregateHashTable);
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

void HashAggregateSharedState::appendTuple(std::span<uint8_t> tuple, common::hash_t hash) {
    auto& partition = globalPartitions[(hash >> shiftForPartitioning) % globalPartitions.size()];
    while (true) {
        auto* block = partition.headBlock.load();
        KU_ASSERT(tuple.size() == block->table.getTableSchema()->getNumBytesPerTuple());
        auto posToWrite = block->numTuplesReserved++;
        if (posToWrite < block->table.getNumTuplesPerBlock()) {
            memcpy(block->table.getTuple(posToWrite), tuple.data(), tuple.size());
            block->numTuplesWritten++;
            return;
        } else {
            // No more space in the block, allocate and replace it
            auto* newBlock = new Partition::TupleBlock(memoryManager, aggInfo.tableSchema.copy());
            if (partition.headBlock.compare_exchange_strong(block, newBlock)) {
                // TODO(bmwinger): if the queuedTuples has at least a certain size (benchmark to see
                // if there's a benefit to waiting for multiple blocks) then cycle through the queue
                // and flush any blocks which have been fully written
                partition.queuedTuples.push(block);
            } else {
                // If the block was replaced by another thread, discard the block we created and try
                // again with the block allocated by the other thread
                delete newBlock;
            }
        }
    }
}

void HashAggregateSharedState::finalizeAggregateHashTable(
    const AggregateHashTable& localHashTable) {
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
                partition.hashTable =
                    std::make_unique<AggregateHashTable>(localHashTable.createEmptyCopy());
            }
            std::vector<Partition::TupleBlock*> partitionsToMerge;
            partitionsToMerge.reserve(partition.queuedTuples.approxSize());
            Partition::TupleBlock* partitionToMerge = nullptr;
            // Over-estimate the total number of distinct groups using the total number of tuples
            // after the per-thread pre-aggregation
            // This will probably use more memory than necessary, but will greatly reduce the need
            // to resize the hash table. It will only use memory proportional to what is already
            // used by the queuedTuples already, and the hash slots are usually small in comparison
            // to the full tuple data.
            auto headBlock = partition.headBlock.load();
            KU_ASSERT(headBlock != nullptr);
            partition.hashTable->resizeHashTableIfNecessary(
                partition.queuedTuples.approxSize() * headBlock->table.getNumTuplesPerBlock() +
                headBlock->numTuplesWritten);
            while (partition.queuedTuples.pop(partitionToMerge)) {
                KU_ASSERT(partitionToMerge->numTuplesWritten ==
                          partitionToMerge->table.getNumTuplesPerBlock());
                partition.hashTable->merge(std::move(partitionToMerge->table));
                delete partitionToMerge;
            }
            if (headBlock->numTuplesWritten > 0) {
                headBlock->table.resize(headBlock->numTuplesWritten);
                partition.hashTable->merge(std::move(headBlock->table));
            }
            delete headBlock;
            partition.headBlock = nullptr;
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
        KU_ASSERT(partition.queuedTuples.approxSize() == 0);
    });
}

} // namespace processor
} // namespace kuzu
