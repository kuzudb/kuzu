#include "processor/operator/partitioner.h"

#include "binder/expression/expression_util.h"
#include "common/constants.h"
#include "processor/execution_context.h"
#include "processor/operator/persistent/rel_batch_insert.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::string PartitionerPrintInfo::toString() const {
    std::string result = "Indexes: ";
    result += binder::ExpressionUtil::toString(expressions);
    return result;
}

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

void PartitionerSharedState::initialize(const PartitionerDataInfo& dataInfo,
    main::ClientContext* clientContext) {
    maxNodeOffsets[0] = srcNodeTable->getNumTotalRows(clientContext->getTx());
    maxNodeOffsets[1] = dstNodeTable->getNumTotalRows(clientContext->getTx());
    numPartitions[0] = getNumPartitions(maxNodeOffsets[0]);
    numPartitions[1] = getNumPartitions(maxNodeOffsets[1]);
    Partitioner::initializePartitioningStates(dataInfo, partitioningBuffers, numPartitions);
}

partition_idx_t PartitionerSharedState::getNextPartition(idx_t partitioningIdx,
    RelBatchInsertProgressSharedState& progressSharedState) {
    auto nextPartitionIdxToReturn = nextPartitionIdx++;
    if (nextPartitionIdxToReturn >= numPartitions[partitioningIdx]) {
        return INVALID_PARTITION_IDX;
    }
    progressSharedState.partitionsDone++;
    return nextPartitionIdxToReturn;
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

void PartitioningBuffer::merge(std::unique_ptr<PartitioningBuffer> localPartitioningState) const {
    KU_ASSERT(partitions.size() == localPartitioningState->partitions.size());
    for (auto partitionIdx = 0u; partitionIdx < partitions.size(); partitionIdx++) {
        auto& sharedPartition = partitions[partitionIdx];
        auto& localPartition = localPartitioningState->partitions[partitionIdx];
        sharedPartition->merge(*localPartition);
    }
}

Partitioner::Partitioner(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
    PartitionerInfo info, PartitionerDataInfo dataInfo,
    std::shared_ptr<PartitionerSharedState> sharedState, std::unique_ptr<PhysicalOperator> child,
    uint32_t id, std::unique_ptr<OPPrintInfo> printInfo)
    : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::PARTITIONER, std::move(child), id,
          std::move(printInfo)},
      dataInfo{std::move(dataInfo)}, info{std::move(info)}, sharedState{std::move(sharedState)} {
    partitionIdxes = std::make_unique<ValueVector>(LogicalTypeID::INT64);
}

void Partitioner::initGlobalStateInternal(ExecutionContext* context) {
    sharedState->initialize(dataInfo, context->clientContext);
}

void Partitioner::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    localState = std::make_unique<PartitionerLocalState>();
    initializePartitioningStates(dataInfo, localState->partitioningBuffers,
        sharedState->numPartitions);
    for (const auto& evaluator : dataInfo.columnEvaluators) {
        evaluator->init(*resultSet, context->clientContext);
    }
}

DataChunk Partitioner::constructDataChunk(const std::shared_ptr<DataChunkState>& state) const {
    const auto numColumns = dataInfo.columnEvaluators.size();
    DataChunk dataChunk(numColumns, state);
    for (auto i = 0u; i < numColumns; ++i) {
        auto& evaluator = dataInfo.columnEvaluators[i];
        dataChunk.insert(i, evaluator->resultVector);
    }
    return dataChunk;
}

void Partitioner::initializePartitioningStates(const PartitionerDataInfo& dataInfo,
    std::vector<std::unique_ptr<PartitioningBuffer>>& partitioningBuffers,
    const std::array<partition_idx_t, PartitionerSharedState::DIRECTIONS>& numPartitions) {
    partitioningBuffers.resize(numPartitions.size());
    for (auto partitioningIdx = 0u; partitioningIdx < numPartitions.size(); partitioningIdx++) {
        const auto numPartition = numPartitions[partitioningIdx];
        auto partitioningBuffer = std::make_unique<PartitioningBuffer>();
        partitioningBuffer->partitions.reserve(numPartition);
        for (auto i = 0u; i < numPartition; i++) {
            partitioningBuffer->partitions.push_back(
                std::make_unique<InMemChunkedNodeGroupCollection>(
                    LogicalType::copy(dataInfo.columnTypes)));
        }
        partitioningBuffers[partitioningIdx] = std::move(partitioningBuffer);
    }
}

void Partitioner::executeInternal(ExecutionContext* context) {
    const auto relOffsetVector = resultSet->getValueVector(info.relOffsetDataPos);
    while (children[0]->getNextTuple(context)) {
        KU_ASSERT(dataInfo.columnEvaluators.size() >= 1);
        const auto numRels = relOffsetVector->state->getSelVector().getSelSize();
        for (auto i = 0u; i < dataInfo.evaluateTypes.size(); ++i) {
            auto evaluator = dataInfo.columnEvaluators[i].get();
            switch (dataInfo.evaluateTypes[i]) {
            case ColumnEvaluateType::DEFAULT: {
                evaluator->getLocalStateUnsafe().count = numRels;
                evaluator->evaluate();
            } break;
            default: {
                evaluator->evaluate();
            }
            }
        }
        auto currentRelOffset = sharedState->relTable->reserveRelOffsets(numRels);
        for (auto i = 0u; i < numRels; i++) {
            const auto pos = relOffsetVector->state->getSelVector()[i];
            relOffsetVector->setValue<offset_t>(pos, currentRelOffset++);
        }
        for (auto partitioningIdx = 0u; partitioningIdx < info.infos.size(); partitioningIdx++) {
            auto& partitionInfo = info.infos[partitioningIdx];
            auto keyVector = dataInfo.columnEvaluators[partitionInfo.keyIdx]->resultVector;
            partitionIdxes->state = keyVector->state;
            partitionInfo.partitionerFunc(keyVector.get(), partitionIdxes.get());
            auto chunkToCopyFrom = constructDataChunk(keyVector->state);
            copyDataToPartitions(*context->clientContext->getMemoryManager(), partitioningIdx,
                std::move(chunkToCopyFrom));
        }
    }
    sharedState->merge(std::move(localState->partitioningBuffers));
}

void Partitioner::copyDataToPartitions(MemoryManager& memoryManager,
    partition_idx_t partitioningIdx, DataChunk chunkToCopyFrom) const {
    std::vector<ValueVector*> vectorsToAppend;
    vectorsToAppend.reserve(chunkToCopyFrom.getNumValueVectors());
    for (auto j = 0u; j < chunkToCopyFrom.getNumValueVectors(); j++) {
        vectorsToAppend.push_back(&chunkToCopyFrom.getValueVectorMutable(j));
    }
    for (auto i = 0u; i < chunkToCopyFrom.state->getSelVector().getSelSize(); i++) {
        const auto posToCopyFrom = chunkToCopyFrom.state->getSelVector()[i];
        const auto partitionIdx = partitionIdxes->getValue<partition_idx_t>(posToCopyFrom);
        KU_ASSERT(
            partitionIdx < localState->getPartitioningBuffer(partitioningIdx)->partitions.size());
        const auto& partition =
            localState->getPartitioningBuffer(partitioningIdx)->partitions[partitionIdx];
        partition->append(memoryManager, vectorsToAppend, i, 1);
    }
}

std::unique_ptr<PhysicalOperator> Partitioner::clone() {
    return std::make_unique<Partitioner>(resultSetDescriptor->copy(), info.copy(), dataInfo.copy(),
        sharedState, children[0]->clone(), id, printInfo->copy());
}

} // namespace processor
} // namespace kuzu
