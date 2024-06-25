#include "processor/operator/partitioner.h"

#include "common/constants.h"
#include "common/data_chunk/sel_vector.h"
#include "processor/execution_context.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void PartitionerFunctions::partitionRelData(ValueVector* key, ValueVector* partitionIdxes) {
    KU_ASSERT(key->state == partitionIdxes->state &&
              key->dataType.getPhysicalType() == PhysicalTypeID::INT64);
    for (auto i = 0u; i < key->state->getSelVector().getSelSize(); i++) {
        const auto pos = key->state->getSelVector()[i];
        const partition_idx_t partitionIdx =
            key->getValue<offset_t>(pos) >> StorageConstants::NODE_GROUP_SIZE_LOG2;
        partitionIdxes->setValue(pos, partitionIdx);
    }
}

static partition_idx_t getNumPartitions(offset_t maxOffset) {
    return (maxOffset + StorageConstants::NODE_GROUP_SIZE) / StorageConstants::NODE_GROUP_SIZE;
}

void PartitionerSharedState::initialize(
    const std::vector<std::unique_ptr<PartitioningInfo>>& infos) {
    maxNodeOffsets.resize(2);
    maxNodeOffsets[0] = srcNodeTable->getNumRows();
    maxNodeOffsets[1] = dstNodeTable->getNumRows();
    numPartitions.resize(2);
    numPartitions[0] = getNumPartitions(maxNodeOffsets[0]);
    numPartitions[1] = getNumPartitions(maxNodeOffsets[1]);
    Partitioner::initializePartitioningStates(infos, partitioningBuffers, numPartitions);
}

partition_idx_t PartitionerSharedState::getNextPartition(idx_t partitioningIdx) {
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
        auto& sharedPartition = partitions[partitionIdx];
        auto& localPartition = localPartitioningState->partitions[partitionIdx];
        sharedPartition->merge(std::move(localPartition));
    }
}

Partitioner::Partitioner(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
    PartitionerInfo info, std::shared_ptr<PartitionerSharedState> sharedState,
    std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
    : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::PARTITIONER, std::move(child), id,
          paramsString},
      info{std::move(info)}, sharedState{std::move(sharedState)} {
    partitionIdxes = std::make_unique<ValueVector>(LogicalTypeID::INT64);
}

void Partitioner::initGlobalStateInternal(ExecutionContext* /*context*/) {
    sharedState->initialize(info.infos);
}

void Partitioner::initLocalStateInternal(ResultSet* /*resultSet*/, ExecutionContext* /*context*/) {
    localState = std::make_unique<PartitionerLocalState>();
    initializePartitioningStates(info.infos, localState->partitioningBuffers,
        sharedState->numPartitions);
}

DataChunk Partitioner::constructDataChunk(const std::vector<DataPos>& columnPositions,
    const std::vector<LogicalType>& columnTypes, const ResultSet& resultSet,
    const std::shared_ptr<DataChunkState>& state) {
    DataChunk dataChunk(columnTypes.size(), state);
    for (auto i = 0u; i < columnPositions.size(); i++) {
        auto pos = columnPositions[i];
        if (pos.isValid()) {
            dataChunk.insert(i, resultSet.getValueVector(pos));
        } else {
            auto nullVector = std::make_shared<ValueVector>(columnTypes[i]);
            nullVector->setAllNull();
            dataChunk.insert(i, nullVector);
        }
    }
    return dataChunk;
}

void Partitioner::initializePartitioningStates(
    const std::vector<std::unique_ptr<PartitioningInfo>>& infos,
    std::vector<std::unique_ptr<PartitioningBuffer>>& partitioningBuffers,
    const std::vector<partition_idx_t>& numPartitions) {
    partitioningBuffers.resize(numPartitions.size());
    for (auto partitioningIdx = 0u; partitioningIdx < numPartitions.size(); partitioningIdx++) {
        const auto numPartition = numPartitions[partitioningIdx];
        auto partitioningBuffer = std::make_unique<PartitioningBuffer>();
        partitioningBuffer->partitions.reserve(numPartition);
        for (auto i = 0u; i < numPartition; i++) {
            partitioningBuffer->partitions.push_back(std::make_unique<ChunkedNodeGroupCollection>(
                ResidencyState::IN_MEMORY, infos[partitioningIdx]->columnTypes));
        }
        partitioningBuffers[partitioningIdx] = std::move(partitioningBuffer);
    }
}

void Partitioner::executeInternal(ExecutionContext* context) {
    const auto relOffsetVector = resultSet->getValueVector(info.relOffsetDataPos);
    while (children[0]->getNextTuple(context)) {
        const auto numRels = relOffsetVector->state->getSelVector().getSelSize();
        auto currentRelOffset = sharedState->relTable->reserveRelOffsets(numRels);
        for (auto i = 0u; i < numRels; i++) {
            const auto pos = relOffsetVector->state->getSelVector()[i];
            relOffsetVector->setValue<offset_t>(pos, currentRelOffset++);
        }
        for (auto partitioningIdx = 0u; partitioningIdx < info.infos.size(); partitioningIdx++) {
            const auto partitioningInfo = info.infos[partitioningIdx].get();
            auto keyVector = resultSet->getValueVector(partitioningInfo->keyDataPos);
            partitionIdxes->state = resultSet->getValueVector(partitioningInfo->keyDataPos)->state;
            partitioningInfo->partitionerFunc(keyVector.get(), partitionIdxes.get());
            auto chunkToCopyFrom = constructDataChunk(partitioningInfo->columnDataPositions,
                partitioningInfo->columnTypes, *resultSet, keyVector->state);
            copyDataToPartitions(partitioningIdx, std::move(chunkToCopyFrom));
        }
    }
    sharedState->merge(std::move(localState->partitioningBuffers));
}

void Partitioner::copyDataToPartitions(partition_idx_t partitioningIdx,
    DataChunk chunkToCopyFrom) const {
    std::vector<ValueVector*> vectorsToAppend;
    vectorsToAppend.reserve(chunkToCopyFrom.getNumValueVectors());
    for (auto j = 0u; j < chunkToCopyFrom.getNumValueVectors(); j++) {
        vectorsToAppend.push_back(chunkToCopyFrom.getValueVector(j).get());
    }
    for (auto i = 0u; i < chunkToCopyFrom.state->getSelVector().getSelSize(); i++) {
        const auto posToCopyFrom = chunkToCopyFrom.state->getSelVector()[i];
        const auto partitionIdx = partitionIdxes->getValue<partition_idx_t>(posToCopyFrom);
        KU_ASSERT(
            partitionIdx < localState->getPartitioningBuffer(partitioningIdx)->partitions.size());
        const auto& partition =
            localState->getPartitioningBuffer(partitioningIdx)->partitions[partitionIdx];
        partition->append(&transaction::DUMMY_WRITE_TRANSACTION, vectorsToAppend, posToCopyFrom, 1);
    }
}

std::unique_ptr<PhysicalOperator> Partitioner::clone() {
    auto copiedInfos = info.copy();
    return std::make_unique<Partitioner>(resultSetDescriptor->copy(), std::move(copiedInfos),
        sharedState, children[0]->clone(), id, paramsString);
}

} // namespace processor
} // namespace kuzu
