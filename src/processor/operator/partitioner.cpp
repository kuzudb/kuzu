#include "processor/operator/partitioner.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void PartitionerFunctions::partitionRelData(ValueVector* key, ValueVector* partitionIdxes) {
    KU_ASSERT(key->state == partitionIdxes->state &&
              key->dataType.getPhysicalType() == PhysicalTypeID::INT64);
    for (auto i = 0u; i < key->state->selVector->selectedSize; i++) {
        auto pos = key->state->selVector->selectedPositions[i];
        partition_idx_t partitionIdx =
            key->getValue<offset_t>(pos) >> StorageConstants::NODE_GROUP_SIZE_LOG2;
        partitionIdxes->setValue(pos, partitionIdx);
    }
}

std::vector<std::unique_ptr<PartitioningInfo>> PartitioningInfo::copy(
    const std::vector<std::unique_ptr<PartitioningInfo>>& other) {
    std::vector<std::unique_ptr<PartitioningInfo>> result;
    result.reserve(other.size());
    for (auto& otherInfo : other) {
        result.push_back(otherInfo->copy());
    }
    return result;
}

void PartitionerSharedState::initialize() {
    KU_ASSERT(maxNodeOffsets.size() == 2);
    numPartitions.resize(maxNodeOffsets.size());
    for (auto i = 0u; i < maxNodeOffsets.size(); i++) {
        numPartitions[i] = (maxNodeOffsets[i] + StorageConstants::NODE_GROUP_SIZE) /
                           StorageConstants::NODE_GROUP_SIZE;
    }
    Partitioner::initializePartitioningStates(partitioningBuffers, numPartitions, mm);
}

partition_idx_t PartitionerSharedState::getNextPartition(vector_idx_t partitioningIdx) {
    std::unique_lock xLck{mtx};
    if (nextPartitionIdx >= numPartitions[partitioningIdx]) {
        return INVALID_PARTITION_IDX;
    }
    return nextPartitionIdx++;
}

void PartitionerSharedState::resetState() {
    nextPartitionIdx = 0;
}

void PartitionerSharedState::merge(
    std::vector<std::unique_ptr<PartitioningBuffer>> localPartitioningStates) {
    std::unique_lock xLck{mtx};
    KU_ASSERT(partitioningBuffers.size() == localPartitioningStates.size());
    for (auto partitioningIdx = 0u; partitioningIdx < partitioningBuffers.size();
         partitioningIdx++) {
        partitioningBuffers[partitioningIdx]->merge(
            std::move(localPartitioningStates[partitioningIdx]));
    }
}

void PartitioningBuffer::merge(std::unique_ptr<PartitioningBuffer> localPartitioningState) {
    KU_ASSERT(partitions.size() == localPartitioningState->partitions.size());
    for (auto partitionIdx = 0u; partitionIdx < partitions.size(); partitionIdx++) {
        auto sharedPartition = partitions[partitionIdx].get();
        auto localPartition = localPartitioningState->partitions[partitionIdx].get();
        sharedPartition->merge(localPartition);
    }
}

Partitioner::Partitioner(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
    std::vector<std::unique_ptr<PartitioningInfo>> infos,
    std::shared_ptr<PartitionerSharedState> sharedState, std::unique_ptr<PhysicalOperator> child,
    uint32_t id, const std::string& paramsString)
    : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::PARTITIONER, std::move(child), id,
          paramsString},
      infos{std::move(infos)}, sharedState{std::move(sharedState)} {
    partitionIdxes = std::make_unique<ValueVector>(LogicalTypeID::INT64);
}

void Partitioner::initGlobalStateInternal(ExecutionContext* /*context*/) {
    sharedState->initialize();
}

void Partitioner::initLocalStateInternal(ResultSet* /*resultSet*/, ExecutionContext* context) {
    localState = std::make_unique<PartitionerLocalState>();
    initializePartitioningStates(
        localState->partitioningBuffers, sharedState->numPartitions, context->memoryManager);
}

static void constructDataChunk(DataChunk* dataChunk, const std::vector<DataPos>& columnPositions,
    const logical_types_t& columnTypes, const ResultSet& resultSet) {
    for (auto i = 0u; i < columnPositions.size(); i++) {
        auto pos = columnPositions[i];
        if (pos.isValid()) {
            dataChunk->insert(i, resultSet.getValueVector(pos));
        } else {
            auto columnType = columnTypes[i].get();
            auto nullVector = std::make_shared<ValueVector>(*columnType);
            nullVector->setAllNull();
            dataChunk->insert(i, nullVector);
        }
    }
}

void Partitioner::initializePartitioningStates(
    std::vector<std::unique_ptr<PartitioningBuffer>>& partitioningBuffers,
    std::vector<common::partition_idx_t> numPartitions, MemoryManager* mm) {
    partitioningBuffers.resize(numPartitions.size());
    for (auto partitioningIdx = 0u; partitioningIdx < numPartitions.size(); partitioningIdx++) {
        auto numPartition = numPartitions[partitioningIdx];
        auto partitioningBuffer = std::make_unique<PartitioningBuffer>();
        partitioningBuffer->partitions.reserve(numPartition);
        for (auto i = 0u; i < numPartition; i++) {
            partitioningBuffer->partitions.push_back(std::make_unique<DataChunkCollection>(mm));
        }
        partitioningBuffers[partitioningIdx] = std::move(partitioningBuffer);
    }
}

void Partitioner::executeInternal(ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
        for (auto partitioningIdx = 0u; partitioningIdx < infos.size(); partitioningIdx++) {
            auto info = infos[partitioningIdx].get();
            auto keyVector = resultSet->getValueVector(info->keyDataPos);
            partitionIdxes->state = resultSet->getValueVector(info->keyDataPos)->state;
            info->partitionerFunc(keyVector.get(), partitionIdxes.get());
            auto columnDataChunk =
                std::make_unique<DataChunk>(info->columnTypes.size(), keyVector->state);
            constructDataChunk(
                columnDataChunk.get(), info->columnDataPositions, info->columnTypes, *resultSet);
            copyDataToPartitions(partitioningIdx, columnDataChunk.get());
        }
    }
    sharedState->merge(std::move(localState->partitioningBuffers));
}

void Partitioner::copyDataToPartitions(
    partition_idx_t partitioningIdx, DataChunk* chunkToCopyFrom) {
    auto originalChunkState = chunkToCopyFrom->state;
    chunkToCopyFrom->state = std::make_shared<DataChunkState>(1 /* capacity */);
    chunkToCopyFrom->state->selVector->resetSelectorToValuePosBufferWithSize(1 /* size */);
    for (auto i = 0u; i < originalChunkState->selVector->selectedSize; i++) {
        auto posToCopyFrom = originalChunkState->selVector->selectedPositions[i];
        auto partitionIdx = partitionIdxes->getValue<partition_idx_t>(posToCopyFrom);
        KU_ASSERT(
            partitionIdx < localState->getPartitioningBuffer(partitioningIdx)->partitions.size());
        auto partition =
            localState->getPartitioningBuffer(partitioningIdx)->partitions[partitionIdx].get();
        chunkToCopyFrom->state->selVector->selectedPositions[0] = posToCopyFrom;
        partition->append(*chunkToCopyFrom);
    }
}

std::unique_ptr<PhysicalOperator> Partitioner::clone() {
    auto copiedInfos = PartitioningInfo::copy(infos);
    return std::make_unique<Partitioner>(resultSetDescriptor->copy(), std::move(copiedInfos),
        sharedState, children[0]->clone(), id, paramsString);
}

} // namespace processor
} // namespace kuzu
