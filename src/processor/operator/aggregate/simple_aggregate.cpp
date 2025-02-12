#include "processor/operator/aggregate/simple_aggregate.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <thread>

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

SimpleAggregateSharedState::SimpleAggregateSharedState(main::ClientContext* context,
    const std::vector<AggregateFunction>& aggregateFunctions,
    const std::vector<AggregateInfo>& aggInfos)
    : BaseAggregateSharedState{aggregateFunctions,
          // Only distinct functions need partitioning
          static_cast<size_t>(context->getMaxNumThreadForExec())},
      globalPartitions{std::any_of(aggregateFunctions.begin(), aggregateFunctions.end(),
                           [&](auto& func) { return func.isDistinct; }) ?
                           static_cast<size_t>(context->getMaxNumThreadForExec()) :
                           0} {
    for (size_t funcIdx = 0; funcIdx < this->aggregateFunctions.size(); funcIdx++) {
        auto& aggregateFunction = this->aggregateFunctions[funcIdx];
        globalAggregateStates.push_back(aggregateFunction.createInitialNullAggregateState());
        partitioningData.emplace_back(this, funcIdx);
        for (auto& partition : globalPartitions) {
            if (aggregateFunction.isDistinct) {
                partition.distinctTables.emplace_back(
                    std::make_pair<std::unique_ptr<AggregateHashTable>>(nullptr,
                        std::make_unique<HashTableQueue>(context->getMemoryManager(),
                            AggregateHashTableUtils::getTableSchemaForKeys(
                                std::vector<LogicalType>{},
                                aggInfos[funcIdx].distinctAggKeyType))));
            } else {
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
        // Distinct functions will be combined accross the partitions in finalizeAggregateStates
        if (!aggregateFunctions[i].isDistinct) {
            aggregateFunctions[i].combineState((uint8_t*)globalAggregateStates[i].get(),
                (uint8_t*)localAggregateStates[i].get(), memoryManager);
        }
    }
}

void SimpleAggregateSharedState::finalizeAggregateStates() {
    std::unique_lock lck{mtx};
    for (auto i = 0u; i < aggregateFunctions.size(); ++i) {
        aggregateFunctions[i].finalizeState((uint8_t*)globalAggregateStates[i].get());
    }
}

void SimpleAggregate::updateDistinctAggregateStates(storage::MemoryManager* memoryManager) {
    for (auto i = 0u; i < aggregateFunctions.size(); ++i) {
        if (aggregateFunctions[i].isDistinct) {
            auto state = std::make_shared<DataChunkState>();
            // TODO(bmwinger): this should probably be done inside the shared state, but that
            // doesn't have the types. Maybe just pass them as an argument with the memorymanager
            ValueVector aggregateVector(aggInfos[i].distinctAggKeyType.copy(), memoryManager,
                state);
            for (auto& partition : sharedState->globalPartitions) {
                KU_ASSERT(partition.finalized);
                const auto& [hashTable, queue] = partition.distinctTables[i];
                if (!hashTable) {
                    continue;
                }
                const auto& ft = hashTable->getFactorizedTable();
                ft_tuple_idx_t startTupleIdx = 0;
                ft_tuple_idx_t numTuplesToScan =
                    std::min(DEFAULT_VECTOR_CAPACITY, ft->getNumTuples() - startTupleIdx);
                std::array<uint32_t, 1> colIdxToScan = {0};
                std::array<ValueVector*, 1> vectors = {&aggregateVector};
                while (numTuplesToScan > 0) {
                    ft->scan(vectors, startTupleIdx, numTuplesToScan, colIdxToScan);
                    aggregateFunctions[i].updateAllState(
                        (uint8_t*)sharedState->getAggregateState(i), &aggregateVector,
                        1 /*multiplicity*/, memoryManager);
                    startTupleIdx += numTuplesToScan;
                    numTuplesToScan =
                        std::min(DEFAULT_VECTOR_CAPACITY, ft->getNumTuples() - startTupleIdx);
                }
            }
        }
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
            distinctHT =
                std::make_unique<PartitioningAggregateHashTable>(&sharedState->partitioningData[i],
                    *mm, std::move(keyTypes), std::vector<LogicalType>{} /* empty payload*/,
                    std::vector<function::AggregateFunction>{},
                    std::vector<LogicalType>{} /*empty distinct keys*/,
                    AggregateHashTableUtils::getTableSchemaForKeys(std::vector<LogicalType>{},
                        aggInfos[i].distinctAggKeyType));
        } else {
            distinctHT = nullptr;
        }
        distinctHashTables.push_back(std::move(distinctHT));
    };
    sharedState->registerThread();
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
    if (hasDistinct) {
        for (auto& hashTable : distinctHashTables) {
            hashTable->mergeAll();
        }
        sharedState->setThreadFinishedProducing();
        while (!sharedState->allThreadsFinishedProducing()) {
            std::this_thread::sleep_for(std::chrono::microseconds(500));
        }
        sharedState->finalizeDistinctHashTables(distinctHashTables);
    }
    sharedState->combineAggregateStates(localAggregateStates, memoryManager);
}

void SimpleAggregateSharedState::finalizeDistinctHashTables(
    const std::vector<std::unique_ptr<PartitioningAggregateHashTable>>& localDistinctHashTables) {
    for (auto& partition : globalPartitions) {
        if (!partition.finalized && partition.mtx.try_lock()) {
            if (partition.finalized) {
                // If there was a data race in the above && a thread may get through after another
                // thread has finalized this partition
                partition.mtx.unlock();
                continue;
            }
            for (size_t i = 0; i < partition.distinctTables.size(); i++) {
                auto& [hashTable, queue] = partition.distinctTables[i];
                if (queue) {
                    if (!hashTable) {
                        hashTable = std::make_unique<AggregateHashTable>(
                            localDistinctHashTables[i]->createEmptyCopy());
                    }
                    queue->mergeInto(*hashTable);
                }
            }

            partition.finalized = true;
            partition.mtx.unlock();
        }
    }
}

void SimpleAggregate::computeDistinctAggregate(AggregateHashTable* distinctHT,
    function::AggregateFunction* function, AggregateInput* input, function::AggregateState* state,
    storage::MemoryManager* memoryManager) {
    auto multiplicity = 1; // Distinct aggregate should ignore multiplicity.
    if (distinctHT->insertAggregateValueIfDistinctForGroupByKeys(std::vector<ValueVector*>{},
            input->aggregateVector)) {
        auto pos = input->aggregateVector->state->getSelVector()[0];
        if (!input->aggregateVector->isNull(pos)) {
            function->updatePosState((uint8_t*)state, input->aggregateVector, multiplicity, pos,
                memoryManager);
        }
    }
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

} // namespace processor
} // namespace kuzu
