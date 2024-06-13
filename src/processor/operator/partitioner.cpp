#include "processor/operator/partitioner.h"

#include "common/constants.h"
#include "common/data_chunk/sel_vector.h"
#include "processor/execution_context.h"
#include "storage/store/node_table.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void PartitionerFunctions::partitionRelData(ValueVector* key, ValueVector* partitionIdxes) {
    KU_ASSERT(key->state == partitionIdxes->state &&
              key->dataType.getPhysicalType() == PhysicalTypeID::INT64);
    for (auto i = 0u; i < key->state->getSelVector().getSelSize(); i++) {
        auto pos = key->state->getSelVector()[i];
        partition_idx_t partitionIdx =
            key->getValue<offset_t>(pos) >> StorageConstants::NODE_GROUP_SIZE_LOG2;
        partitionIdxes->setValue(pos, partitionIdx);
    }
}

std::vector<PartitioningInfo> PartitioningInfo::copy(const std::vector<PartitioningInfo>& other) {
    std::vector<PartitioningInfo> result;
    result.reserve(other.size());
    for (auto& otherInfo : other) {
        result.push_back(otherInfo.copy());
    }
    return result;
}

static partition_idx_t getNumPartitions(offset_t maxOffset) {
    return (maxOffset + StorageConstants::NODE_GROUP_SIZE) / StorageConstants::NODE_GROUP_SIZE;
}

void PartitionerSharedState::initialize(PartitionerDataInfo& dataInfo) {
    maxNodeOffsets.resize(2);
    maxNodeOffsets[0] =
        srcNodeTable->getMaxNodeOffset(transaction::Transaction::getDummyWriteTrx().get());
    maxNodeOffsets[1] =
        dstNodeTable->getMaxNodeOffset(transaction::Transaction::getDummyWriteTrx().get());
    numPartitions.resize(2);
    numPartitions[0] = getNumPartitions(maxNodeOffsets[0]);
    numPartitions[1] = getNumPartitions(maxNodeOffsets[1]);
    Partitioner::initializePartitioningStates(dataInfo, partitioningBuffers, numPartitions);
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
        sharedPartition.merge(localPartition);
    }
}

Partitioner::Partitioner(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
    std::vector<PartitioningInfo> infos, PartitionerDataInfo dataInfo,
    std::shared_ptr<PartitionerSharedState> sharedState, std::unique_ptr<PhysicalOperator> child,
    uint32_t id, std::unique_ptr<OPPrintInfo> printInfo)
    : Sink{std::move(resultSetDescriptor), type_, std::move(child), id, std::move(printInfo)},
      infos{std::move(infos)}, dataInfo{std::move(dataInfo)}, sharedState{std::move(sharedState)} {
    partitionIdxes = std::make_unique<ValueVector>(LogicalTypeID::INT64);
}

void Partitioner::initGlobalStateInternal(ExecutionContext* /*context*/) {
    sharedState->initialize(dataInfo);
}

void Partitioner::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    localState = std::make_unique<PartitionerLocalState>();
    initializePartitioningStates(dataInfo, localState->partitioningBuffers,
        sharedState->numPartitions);
    for (auto& evaluator : dataInfo.columnEvaluators) {
        evaluator->init(*resultSet, context->clientContext);
    }
}

void Partitioner::evaluateData(const common::sel_t& numTuples) {
    for (auto& evaluator : dataInfo.columnEvaluators) {
        evaluator->getLocalStateUnsafe().count = numTuples;
        evaluator->evaluate();
    }
}

DataChunk Partitioner::constructDataChunk(const std::shared_ptr<DataChunkState>& state) {
    auto numColumns = dataInfo.columnEvaluators.size();
    DataChunk dataChunk(numColumns, state);
    for (auto i = 0u; i < numColumns; ++i) {
        auto& evaluator = dataInfo.columnEvaluators[i];
        dataChunk.insert(i, evaluator->resultVector);
    }
    return dataChunk;
}

void Partitioner::initializePartitioningStates(PartitionerDataInfo& dataInfo,
    std::vector<std::unique_ptr<PartitioningBuffer>>& partitioningBuffers,
    std::vector<partition_idx_t> numPartitions) {
    partitioningBuffers.resize(numPartitions.size());
    for (auto partitioningIdx = 0u; partitioningIdx < numPartitions.size(); partitioningIdx++) {
        auto numPartition = numPartitions[partitioningIdx];
        auto partitioningBuffer = std::make_unique<PartitioningBuffer>();
        partitioningBuffer->partitions.reserve(numPartition);
        for (auto i = 0u; i < numPartition; i++) {
            partitioningBuffer->partitions.emplace_back(LogicalType::copy(dataInfo.columnTypes));
        }
        partitioningBuffers[partitioningIdx] = std::move(partitioningBuffer);
    }
}

void Partitioner::executeInternal(ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
        KU_ASSERT(dataInfo.columnEvaluators.size() >= 1);
        // We get the numTuples from the state of the src column, which is always idx 0
        auto numTuples =
            dataInfo.columnEvaluators[0]->resultVector->state->getSelVector().getSelSize();
        evaluateData(numTuples);
        for (auto partitioningIdx = 0u; partitioningIdx < infos.size(); partitioningIdx++) {
            auto& info = infos[partitioningIdx];
            auto keyVector = dataInfo.columnEvaluators[info.keyIdx]->resultVector;
            partitionIdxes->state = keyVector->state;
            info.partitionerFunc(keyVector.get(), partitionIdxes.get());
            auto chunkToCopyFrom = constructDataChunk(keyVector->state);
            copyDataToPartitions(partitioningIdx, std::move(chunkToCopyFrom));
        }
    }
    sharedState->merge(std::move(localState->partitioningBuffers));
}

void Partitioner::copyDataToPartitions(partition_idx_t partitioningIdx, DataChunk chunkToCopyFrom) {
    std::vector<ValueVector*> vectorsToAppend;
    vectorsToAppend.reserve(chunkToCopyFrom.getNumValueVectors());
    for (auto j = 0u; j < chunkToCopyFrom.getNumValueVectors(); j++) {
        vectorsToAppend.push_back(chunkToCopyFrom.getValueVector(j).get());
    }
    SelectionVector selVector(1);
    selVector.setToFiltered(1);
    for (auto i = 0u; i < chunkToCopyFrom.state->getSelVector().getSelSize(); i++) {
        auto posToCopyFrom = chunkToCopyFrom.state->getSelVector()[i];
        auto partitionIdx = partitionIdxes->getValue<partition_idx_t>(posToCopyFrom);
        KU_ASSERT(
            partitionIdx < localState->getPartitioningBuffer(partitioningIdx)->partitions.size());
        auto& partition =
            localState->getPartitioningBuffer(partitioningIdx)->partitions[partitionIdx];
        selVector[0] = posToCopyFrom;
        partition.append(vectorsToAppend, selVector);
    }
}

std::unique_ptr<PhysicalOperator> Partitioner::clone() {
    return std::make_unique<Partitioner>(resultSetDescriptor->copy(), PartitioningInfo::copy(infos),
        dataInfo.copy(), sharedState, children[0]->clone(), id, printInfo->copy());
}

} // namespace processor
} // namespace kuzu
