#include "processor/operator/aggregate/base_aggregate.h"

#include "main/client_context.h"
#include "processor/operator/aggregate/aggregate_hash_table.h"

using namespace kuzu::function;

namespace kuzu {
namespace processor {

size_t getNumPartitionsForParallelism(main::ClientContext* context) {
    return context->getMaxNumThreadForExec();
}

BaseAggregateSharedState::BaseAggregateSharedState(
    const std::vector<AggregateFunction>& aggregateFunctions, size_t numPartitions)
    : currentOffset{0}, aggregateFunctions{copyVector(aggregateFunctions)}, numThreads{0},
      // numPartitions - 1 since we want the bit width of the largest value that
      // could be used to index the partitions
      shiftForPartitioning{
          static_cast<uint8_t>(sizeof(common::hash_t) * 8 - std::bit_width(numPartitions - 1))},
      readyForFinalization{false} {}

void BaseAggregate::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* /*context*/) {
    for (auto& info : aggInfos) {
        auto aggregateInput = AggregateInput();
        if (info.aggVectorPos.dataChunkPos == INVALID_DATA_CHUNK_POS) {
            aggregateInput.aggregateVector = nullptr;
        } else {
            aggregateInput.aggregateVector = resultSet->getValueVector(info.aggVectorPos).get();
        }
        for (auto dataChunkPos : info.multiplicityChunksPos) {
            aggregateInput.multiplicityChunks.push_back(
                resultSet->getDataChunk(dataChunkPos).get());
        }
        aggInputs.push_back(std::move(aggregateInput));
    }
}

struct TupleBlock {
    explicit TupleBlock(std::unique_ptr<AggregateFactorizedTable> table)
        : numTuplesReserved{0}, numTuplesWritten{0}, table{std::move(table)} {
        // Start at a fixed capacity of one full block (so that concurrent writes are safe).
        // If it is not filled, we resize it to the actual capacity before writing it to the
        // hashTable
        this->table->resize(this->table->getNumTuplesPerBlock());
    }
    // numTuplesReserved may be greater than the capacity of the factorizedTable
    // if threads try to write to it while a new block is being allocated
    // So it should not be relied on for anything other than reserving tuples
    std::atomic<uint64_t> numTuplesReserved;
    // Set after the tuple has been written to the block.
    // Once numTuplesWritten == factorizedTable.getNumTuplesPerBlock() all writes have
    // finished
    std::atomic<uint64_t> numTuplesWritten;
    std::unique_ptr<AggregateFactorizedTable> table;
};

BaseAggregateSharedState::HashTableQueue::HashTableQueue(
    std::unique_ptr<AggregateFactorizedTable> factorizedTable) {
    headBlock = new TupleBlock(std::move(factorizedTable));
    numTuplesPerBlock = headBlock.load()->table->getNumTuplesPerBlock();
}

BaseAggregateSharedState::HashTableQueue::~HashTableQueue() {
    auto headBlock = this->headBlock.load();
    if (headBlock) {
        headBlock->table->resize(headBlock->numTuplesWritten);
        delete headBlock;
    }
    TupleBlock* block = nullptr;
    while (queuedTuples.pop(block)) {
        block->table->resize(block->numTuplesWritten);
        delete block;
    }
}

bool BaseAggregateSharedState::HashTableQueue::empty() const {
    auto headBlock = this->headBlock.load();
    return (headBlock == nullptr || headBlock->numTuplesReserved == 0) &&
           queuedTuples.approxSize() == 0;
}

std::unique_ptr<BaseAggregateSharedState::HashTableQueue>
BaseAggregateSharedState::HashTableQueue::copy() const {
    return std::make_unique<HashTableQueue>(headBlock.load()->table->createEmptyCopy());
}

void BaseAggregateSharedState::HashTableQueue::moveTuple(std::span<uint8_t> tuple) {
    while (true) {
        auto* block = headBlock.load();
        KU_ASSERT(tuple.size() == block->table->getTableSchema()->getNumBytesPerTuple());
        auto posToWrite = block->numTuplesReserved++;
        if (posToWrite < numTuplesPerBlock) {
            memcpy(block->table->getTuple(posToWrite), tuple.data(), tuple.size());
            // Clear tuple in original table so that the aggregate state doesn't get double-freed
            memset(tuple.data(), 0, tuple.size());
            block->numTuplesWritten++;
            return;
        } else {
            // No more space in the block, allocate and replace it
            auto* newBlock = new TupleBlock(block->table->createEmptyCopy());
            if (headBlock.compare_exchange_strong(block, newBlock)) {
                // TODO(bmwinger): if the queuedTuples has at least a certain size (benchmark to see
                // if there's a benefit to waiting for multiple blocks) then cycle through the queue
                // and flush any blocks which have been fully written
                queuedTuples.push(block);
            } else {
                // If the block was replaced by another thread, discard the block we created and try
                // again with the block allocated by the other thread
                newBlock->table->clear();
                delete newBlock;
            }
        }
    }
}

void BaseAggregateSharedState::HashTableQueue::mergeInto(AggregateHashTable& hashTable) {
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
        queuedTuples.approxSize() * headBlock->table->getNumTuplesPerBlock() +
        headBlock->numTuplesWritten);
    while (queuedTuples.pop(partitionToMerge)) {
        KU_ASSERT(
            partitionToMerge->numTuplesWritten == partitionToMerge->table->getNumTuplesPerBlock());
        hashTable.merge(std::move(partitionToMerge->table));
        delete partitionToMerge;
    }
    if (headBlock->numTuplesWritten > 0) {
        headBlock->table->resize(headBlock->numTuplesWritten);
        hashTable.merge(std::move(headBlock->table));
    } else {
        // Numtuples will be >0, however the AggregateStates were never written to and will not have
        // been properly initialized (null vptr which will be accessed in the destructor)
        headBlock->table->clear();
    }
    delete headBlock;
    this->headBlock = nullptr;
}

AggregateFactorizedTable::~AggregateFactorizedTable() {
    if (numAggregateFunctions > 0 && flatTupleBlockCollection) {
        forEach([&](auto* tuple) {
            for (size_t colIdx = aggStateColIndex;
                 colIdx < aggStateColIndex + numAggregateFunctions; colIdx++) {
                auto state =
                    reinterpret_cast<AggregateState*>(tuple + tableSchema.getColOffset(colIdx));
                if (state->isValid) {
                    state->~AggregateState();
                }
            }
        });
    }
}

} // namespace processor
} // namespace kuzu
