#include "processor/operator/persistent/rel_batch_insert.h"

#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/string_format.h"
#include "processor/operator/partitioner.h"
#include "processor/result/factorized_table.h"
#include "storage/store/column_chunk.h"
#include "storage/store/rel_table.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void RelBatchInsert::initGlobalStateInternal(ExecutionContext* /*context*/) {
    checkIfTableIsEmpty();
    sharedState->logBatchInsertWALRecord();
}

void RelBatchInsert::initLocalStateInternal(
    ResultSet* /*resultSet_*/, ExecutionContext* /*context*/) {
    localState = std::make_unique<RelBatchInsertLocalState>();
    auto relInfo = ku_dynamic_cast<BatchInsertInfo*, RelBatchInsertInfo*>(info.get());
    localState->nodeGroup = NodeGroupFactory::createNodeGroup(
        ColumnDataFormat::CSR, relInfo->columnTypes, relInfo->compressionEnabled);
}

void RelBatchInsert::executeInternal(ExecutionContext* /*context*/) {
    auto relInfo = ku_dynamic_cast<BatchInsertInfo*, RelBatchInsertInfo*>(info.get());
    auto relTable = ku_dynamic_cast<Table*, RelTable*>(sharedState->table);
    auto relLocalState =
        ku_dynamic_cast<BatchInsertLocalState*, RelBatchInsertLocalState*>(localState.get());
    while (true) {
        relLocalState->nodeGroupIdx =
            partitionerSharedState->getNextPartition(relInfo->partitioningIdx);
        if (relLocalState->nodeGroupIdx == INVALID_PARTITION_IDX) {
            // No more partitions left in the partitioning buffer.
            break;
        }
        auto& partitioningBuffer = partitionerSharedState->getPartitionBuffer(
            relInfo->partitioningIdx, relLocalState->nodeGroupIdx);
        auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(relLocalState->nodeGroupIdx);
        for (auto& columns : partitioningBuffer.chunks) {
            setOffsetToWithinNodeGroup(*columns[relInfo->offsetVectorIdx], startNodeOffset);
        }
        // Calculate num of source nodes in this node group.
        // This will be used to set the num of values of the node group.
        auto numNodes = std::min(StorageConstants::NODE_GROUP_SIZE,
            partitionerSharedState->maxNodeOffsets[relInfo->partitioningIdx] - startNodeOffset + 1);
        prepareCSRNodeGroup(
            partitioningBuffer, startNodeOffset, relInfo->offsetVectorIdx, numNodes);
        for (auto& chunk : partitioningBuffer.chunks) {
            localState->nodeGroup->write(chunk, relInfo->offsetVectorIdx);
        }
        localState->nodeGroup->finalize(relLocalState->nodeGroupIdx);
        // Flush node group to table.
        relTable->append(localState->nodeGroup.get(), relInfo->direction);
        sharedState->incrementNumRows(localState->nodeGroup->getNumRows());
        localState->nodeGroup->resetToEmpty();
    }
}

void RelBatchInsert::prepareCSRNodeGroup(PartitioningBuffer::Partition& partition,
    common::offset_t startNodeOffset, vector_idx_t offsetVectorIdx, offset_t numNodes) {
    auto relInfo = ku_dynamic_cast<BatchInsertInfo*, RelBatchInsertInfo*>(info.get());
    auto csrNodeGroup =
        ku_dynamic_cast<ChunkedNodeGroup*, ChunkedCSRNodeGroup*>(localState->nodeGroup.get());
    auto& csrHeader = csrNodeGroup->getCSRHeader();
    csrHeader.setNumValues(numNodes);
    // Populate start csr offsets and lengths for each node.
    auto gaps = populateStartCSROffsetsAndLengths(csrHeader, numNodes, partition, offsetVectorIdx);
    auto invalid = checkRelMultiplicityConstraint(csrHeader);
    if (invalid.has_value()) {
        throw CopyException(ExceptionMessage::violateRelMultiplicityConstraint(
            info->tableEntry->getName(), std::to_string(invalid.value() + startNodeOffset),
            RelDataDirectionUtils::relDirectionToString(relInfo->direction)));
    }
    // Resize csr data column chunks.
    offset_t csrChunkCapacity =
        csrHeader.getEndCSROffset(numNodes - 1) + csrHeader.getCSRLength(numNodes - 1);
    localState->nodeGroup->resizeChunks(csrChunkCapacity);
    for (auto& dataChunk : partition.chunks) {
        auto offsetChunk = dataChunk[offsetVectorIdx].get();
        setOffsetFromCSROffsets(offsetChunk, csrHeader.offset.get());
    }
    populateEndCSROffsets(csrHeader, gaps);
}

void RelBatchInsert::populateEndCSROffsets(
    ChunkedCSRHeader& csrHeader, std::vector<offset_t>& gaps) {
    auto csrOffsets = (offset_t*)csrHeader.offset->getData();
    for (auto i = 0u; i < csrHeader.offset->getNumValues(); i++) {
        csrOffsets[i] += gaps[i];
    }
}

length_t RelBatchInsert::getGapSize(length_t length) {
    // We intentionally leave a gap for empty CSR lists to accommondate for future insertions.
    // Also, for MANY_ONE and ONE_ONE relationships, we should always keep each CSR list as size 1.
    return length == 0 ?
               1 :
               StorageUtils::divideAndRoundUpTo(length, StorageConstants::PACKED_CSR_DENSITY) -
                   length;
}

std::vector<offset_t> RelBatchInsert::populateStartCSROffsetsAndLengths(ChunkedCSRHeader& csrHeader,
    offset_t numNodes, PartitioningBuffer::Partition& partition, vector_idx_t offsetVectorIdx) {
    KU_ASSERT(numNodes == csrHeader.length->getNumValues() &&
              numNodes == csrHeader.offset->getNumValues());
    std::vector<offset_t> gaps;
    gaps.resize(numNodes);
    auto csrOffsets = (offset_t*)csrHeader.offset->getData();
    auto csrLengths = (length_t*)csrHeader.length->getData();
    std::fill(csrLengths, csrLengths + numNodes, 0);
    // Calculate length for each node. Store the num of tuples of node i at csrLengths[i].
    for (auto& chunk : partition.chunks) {
        auto& offsetChunk = chunk[offsetVectorIdx];
        for (auto i = 0u; i < offsetChunk->getNumValues(); i++) {
            auto nodeOffset = offsetChunk->getValue<offset_t>(i);
            KU_ASSERT(nodeOffset < numNodes);
            csrLengths[nodeOffset]++;
        }
    }
    // Calculate gaps for each node.
    for (auto i = 0u; i < numNodes; i++) {
        gaps[i] = getGapSize(csrLengths[i]);
    }
    csrOffsets[0] = 0;
    // Calculate starting offset of each node.
    for (auto i = 1u; i < numNodes; i++) {
        csrOffsets[i] = csrOffsets[i - 1] + csrLengths[i - 1] + gaps[i - 1];
    }
    return gaps;
}

void RelBatchInsert::setOffsetToWithinNodeGroup(ColumnChunk& chunk, offset_t startOffset) {
    KU_ASSERT(chunk.getDataType().getPhysicalType() == PhysicalTypeID::INT64);
    auto offsets = (offset_t*)chunk.getData();
    for (auto i = 0u; i < chunk.getNumValues(); i++) {
        offsets[i] -= startOffset;
    }
}

void RelBatchInsert::setOffsetFromCSROffsets(
    ColumnChunk* nodeOffsetChunk, ColumnChunk* csrOffsetChunk) {
    KU_ASSERT(nodeOffsetChunk->getDataType().getPhysicalType() == PhysicalTypeID::INT64);
    for (auto i = 0u; i < nodeOffsetChunk->getNumValues(); i++) {
        auto nodeOffset = nodeOffsetChunk->getValue<offset_t>(i);
        auto csrOffset = csrOffsetChunk->getValue<offset_t>(nodeOffset);
        nodeOffsetChunk->setValue<offset_t>(csrOffset, i);
        csrOffsetChunk->setValue<offset_t>(csrOffset + 1, nodeOffset);
    }
}

std::optional<common::offset_t> RelBatchInsert::checkRelMultiplicityConstraint(
    const storage::ChunkedCSRHeader& csrHeader) {
    auto relInfo = ku_dynamic_cast<BatchInsertInfo*, RelBatchInsertInfo*>(info.get());
    auto relTableEntry =
        ku_dynamic_cast<catalog::TableCatalogEntry*, catalog::RelTableCatalogEntry*>(
            info->tableEntry);
    if (!relTableEntry->isSingleMultiplicity(relInfo->direction)) {
        return std::nullopt;
    }
    for (auto i = 0u; i < csrHeader.length->getNumValues(); i++) {
        if (csrHeader.length->getValue<length_t>(i) > 1) {
            return i;
        }
    }
    return std::nullopt;
}

void RelBatchInsert::finalize(ExecutionContext* context) {
    auto relInfo = ku_dynamic_cast<BatchInsertInfo*, RelBatchInsertInfo*>(info.get());
    if (relInfo->direction == RelDataDirection::BWD) {
        KU_ASSERT(
            relInfo->partitioningIdx == partitionerSharedState->partitioningBuffers.size() - 1);
        sharedState->setNumTuplesForTable();
        auto outputMsg = stringFormat("{} number of tuples has been copied to table {}.",
            sharedState->getNumRows(), info->tableEntry->getName());
        FactorizedTableUtils::appendStringToTable(
            sharedState->fTable.get(), outputMsg, context->clientContext->getMemoryManager());
    }
    sharedState->numRows.store(0);
    partitionerSharedState->resetState();
    partitionerSharedState->partitioningBuffers[relInfo->partitioningIdx].reset();
}

} // namespace processor
} // namespace kuzu
