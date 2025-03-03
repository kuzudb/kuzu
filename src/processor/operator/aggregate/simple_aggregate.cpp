#include "processor/operator/aggregate/simple_aggregate.h"

#include <algorithm>
#include <cstdint>
#include <vector>

#include "binder/expression/expression_util.h"
#include "common/data_chunk/data_chunk_state.h"
#include "common/system_config.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "function/aggregate_function.h"
#include "main/client_context.h"
#include "processor/execution_context.h"
#include "processor/operator/aggregate/aggregate_hash_table.h"
#include "processor/operator/aggregate/aggregate_input.h"
#include "processor/operator/aggregate/base_aggregate.h"
#include "processor/result/factorized_table.h"
#include "processor/result/factorized_table_schema.h"
#include "storage/buffer_manager/memory_manager.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

std::string SimpleAggregatePrintInfo::toString() const {
    std::string result = "";
    result += "Aggregate: ";
    result += binder::ExpressionUtil::toString(aggregates);
    return result;
}

static bool isAnyFunctionDistinct(const std::vector<AggregateFunction>& functions) {
    return std::any_of(functions.begin(), functions.end(),
        [&](auto& func) { return func.isDistinct; });
}

SimpleAggregateSharedState::SimpleAggregateSharedState(main::ClientContext* context,
    const std::vector<AggregateFunction>& aggregateFunctions,
    const std::vector<AggregateInfo>& aggInfos)
    : BaseAggregateSharedState{aggregateFunctions,
          // Only distinct functions need partitioning
          getNumPartitionsForParallelism(context)},
      hasDistinct{isAnyFunctionDistinct(aggregateFunctions)},
      globalPartitions{hasDistinct ? getNumPartitionsForParallelism(context) : 0} {
    for (size_t funcIdx = 0; funcIdx < this->aggregateFunctions.size(); funcIdx++) {
        auto& aggregateFunction = this->aggregateFunctions[funcIdx];
        globalAggregateStates.push_back(aggregateFunction.createInitialNullAggregateState());
        partitioningData.emplace_back(this, funcIdx);
        if (aggregateFunction.isDistinct) {
            const auto& distinctKeyType = aggInfos[funcIdx].distinctAggKeyType;
            auto schema = AggregateHashTableUtils::getTableSchemaForKeys(std::vector<LogicalType>{},
                aggInfos[funcIdx].distinctAggKeyType);
            for (auto& partition : globalPartitions) {
                std::vector<LogicalType> keyTypes(1);
                keyTypes[0] = distinctKeyType.copy();
                auto hashTable = std::make_unique<AggregateHashTable>(*context->getMemoryManager(),
                    std::move(keyTypes), std::vector<LogicalType>{} /*payloadTypes*/,
                    std::vector<AggregateFunction>{}, std::vector<LogicalType>{}, 0, schema.copy());
                auto queue = std::make_unique<HashTableQueue>(context->getMemoryManager(),
                    AggregateHashTableUtils::getTableSchemaForKeys(std::vector<LogicalType>{},
                        aggInfos[funcIdx].distinctAggKeyType));
                partition.distinctTables.emplace_back(Partition::DistinctData{std::move(hashTable),
                    std::move(queue), aggregateFunction.createInitialNullAggregateState()});
            }
        } else {
            for (auto& partition : globalPartitions) {
                partition.distinctTables.emplace_back();
            }
        }
    }
}

void SimpleAggregateSharedState::combineAggregateStates(
    const std::vector<std::unique_ptr<AggregateState>>& localAggregateStates,
    storage::MemoryManager* memoryManager) {
    KU_ASSERT(localAggregateStates.size() == globalAggregateStates.size());
    std::unique_lock lck{mtx};
    for (auto i = 0u; i < aggregateFunctions.size(); ++i) {
        // Distinct functions will be combined accross the partitions in
        // finalizeAggregateStates
        if (!aggregateFunctions[i].isDistinct) {
            aggregateFunctions[i].combineState(
                reinterpret_cast<uint8_t*>(globalAggregateStates[i].get()),
                reinterpret_cast<uint8_t*>(localAggregateStates[i].get()), memoryManager);
        }
    }
}

void SimpleAggregateSharedState::finalizeAggregateStates(storage::MemoryManager* memoryManager) {
    std::unique_lock lck{mtx};
    for (auto i = 0u; i < aggregateFunctions.size(); ++i) {
        if (aggregateFunctions[i].isDistinct) {
            for (auto& partition : globalPartitions) {
                aggregateFunctions[i].combineState(reinterpret_cast<uint8_t*>(getAggregateState(i)),
                    reinterpret_cast<uint8_t*>(partition.distinctTables[i].state.get()),
                    memoryManager);
            }
        }
        aggregateFunctions[i].finalizeState(
            reinterpret_cast<uint8_t*>(globalAggregateStates[i].get()));
    }
}

std::pair<uint64_t, uint64_t> SimpleAggregateSharedState::getNextRangeToRead() {
    std::unique_lock lck{mtx};
    if (currentOffset >= 1) {
        return std::make_pair(currentOffset, currentOffset);
    }
    auto startOffset = currentOffset;
    currentOffset++;
    return std::make_pair(startOffset, currentOffset);
}

void SimpleAggregateSharedState::SimpleAggregatePartitioningData::appendTuples(
    const FactorizedTable& factorizedTable, ft_col_offset_t hashOffset) {
    KU_ASSERT(sharedState->globalPartitions.size() > 0);
    auto numBytesPerTuple = factorizedTable.getTableSchema()->getNumBytesPerTuple();
    for (ft_tuple_idx_t tupleIdx = 0; tupleIdx < factorizedTable.getNumTuples(); tupleIdx++) {
        auto tuple = factorizedTable.getTuple(tupleIdx);
        auto hash = *reinterpret_cast<common::hash_t*>(tuple + hashOffset);
        auto& partition =
            sharedState->globalPartitions[(hash >> sharedState->shiftForPartitioning) %
                                          sharedState->globalPartitions.size()];
        partition.distinctTables[functionIdx].queue->appendTuple(
            std::span(tuple, numBytesPerTuple));
    }
}

// LCOV_EXCL_START
void SimpleAggregateSharedState::SimpleAggregatePartitioningData::appendDistinctTuple(size_t,
    std::span<uint8_t>, common::hash_t) {
    KU_UNREACHABLE;
}
// LCOV_EXCL_END

void SimpleAggregateSharedState::SimpleAggregatePartitioningData::appendOverflow(
    common::InMemOverflowBuffer&& overflowBuffer) {
    sharedState->overflow.push(
        std::make_unique<common::InMemOverflowBuffer>(std::move(overflowBuffer)));
}

void SimpleAggregateSharedState::finalizePartitions(storage::MemoryManager* memoryManager,
    const std::vector<AggregateInfo>& aggInfos) {
    if (!hasDistinct) {
        return;
    }
    BaseAggregateSharedState::finalizePartitions(globalPartitions, [&](auto& partition) {
        for (size_t i = 0; i < partition.distinctTables.size(); i++) {
            auto& [hashTable, queue, state] = partition.distinctTables[i];
            if (queue) {
                KU_ASSERT(hashTable);
                queue->mergeInto(*hashTable);
            }

            ValueVector aggregateVector(aggInfos[i].distinctAggKeyType.copy(), memoryManager,
                std::make_shared<DataChunkState>());
            const auto& ft = hashTable->getFactorizedTable();
            ft_tuple_idx_t startTupleIdx = 0;
            ft_tuple_idx_t numTuplesToScan =
                std::min(DEFAULT_VECTOR_CAPACITY, ft->getNumTuples() - startTupleIdx);
            std::array<uint32_t, 1> colIdxToScan = {0};
            std::array<ValueVector*, 1> vectors = {&aggregateVector};
            while (numTuplesToScan > 0) {
                ft->scan(vectors, startTupleIdx, numTuplesToScan, colIdxToScan);
                aggregateFunctions[i].updateAllState((uint8_t*)state.get(), &aggregateVector,
                    1 /*multiplicity*/, memoryManager);
                startTupleIdx += numTuplesToScan;
                numTuplesToScan =
                    std::min(DEFAULT_VECTOR_CAPACITY, ft->getNumTuples() - startTupleIdx);
            }
            hashTable.reset();
            queue.reset();
        }
    });
}

void SimpleAggregate::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    BaseAggregate::initLocalStateInternal(resultSet, context);
    for (auto i = 0u; i < aggregateFunctions.size(); ++i) {
        auto& func = aggregateFunctions[i];
        localAggregateStates.push_back(func.createInitialNullAggregateState());
        std::unique_ptr<PartitioningAggregateHashTable> distinctHT;
        if (func.isDistinct) {
            auto mm = context->clientContext->getMemoryManager();
            std::vector<LogicalType> keyTypes;
            keyTypes.push_back(aggInfos[i].distinctAggKeyType.copy());
            distinctHT = std::make_unique<PartitioningAggregateHashTable>(
                &getSharedState().partitioningData[i], *mm, std::move(keyTypes),
                std::vector<LogicalType>{} /* empty payload*/,
                std::vector<function::AggregateFunction>{},
                std::vector<LogicalType>{} /*empty distinct keys*/,
                AggregateHashTableUtils::getTableSchemaForKeys(std::vector<LogicalType>{},
                    aggInfos[i].distinctAggKeyType));
        } else {
            distinctHT = nullptr;
        }
        distinctHashTables.push_back(std::move(distinctHT));
    };
}

void SimpleAggregate::executeInternal(ExecutionContext* context) {
    auto memoryManager = context->clientContext->getMemoryManager();
    while (children[0]->getNextTuple(context)) {
        for (auto i = 0u; i < aggregateFunctions.size(); i++) {
            auto aggregateFunction = &aggregateFunctions[i];
            if (aggregateFunction->isFunctionDistinct()) {
                // Just add distinct value to the hash table. We'll calculate the final hash table
                // once it's been merged into the global state
                distinctHashTables[i].get()->insertAggregateValueIfDistinctForGroupByKeys(
                    std::vector<ValueVector*>{}, aggInputs[i].aggregateVector);
            } else {
                computeAggregate(aggregateFunction, &aggInputs[i], localAggregateStates[i].get(),
                    memoryManager);
            }
        }
    }
    if (getSharedState().hasDistinct) {
        for (auto& hashTable : distinctHashTables) {
            hashTable->mergeIfFull(0 /*tuplesToAdd*/, true /*mergeAll*/);
        }
    }
    getSharedState().combineAggregateStates(localAggregateStates, memoryManager);
}

void SimpleAggregate::computeAggregate(function::AggregateFunction* function, AggregateInput* input,
    function::AggregateState* state, storage::MemoryManager* memoryManager) {
    auto multiplicity = resultSet->multiplicity;
    for (auto dataChunk : input->multiplicityChunks) {
        multiplicity *= dataChunk->state->getSelVector().getSelSize();
    }
    if (input->aggregateVector && input->aggregateVector->state->isFlat()) {
        auto pos = input->aggregateVector->state->getSelVector()[0];
        if (!input->aggregateVector->isNull(pos)) {
            function->updatePosState((uint8_t*)state, input->aggregateVector, multiplicity, pos,
                memoryManager);
        }
    } else {
        function->updateAllState((uint8_t*)state, input->aggregateVector, multiplicity,
            memoryManager);
    }
}

void SimpleAggregateFinalize::finalizeInternal(ExecutionContext* context) {
    sharedState->finalizeAggregateStates(context->clientContext->getMemoryManager());
    if (metrics) {
        metrics->numOutputTuple.incrementByOne();
    }
}

void SimpleAggregateFinalize::executeInternal(ExecutionContext* context) {
    KU_ASSERT(sharedState->isReadyForFinalization());
    sharedState->finalizePartitions(context->clientContext->getMemoryManager(), aggInfos);
}

} // namespace processor
} // namespace kuzu
