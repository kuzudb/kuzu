#include "processor/operator/persistent/rel_batch_insert.h"

#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/string_format.h"
#include "processor/result/factorized_table_util.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/store/column_chunk.h"
#include "storage/store/rel_table.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void RelBatchInsert::initGlobalStateInternal(ExecutionContext* /*context*/) {
    sharedState->logBatchInsertWALRecord();
}

void RelBatchInsert::initLocalStateInternal(ResultSet* /*resultSet_*/,
    ExecutionContext* /*context*/) {
    localState = std::make_unique<RelBatchInsertLocalState>();
    auto relInfo = ku_dynamic_cast<BatchInsertInfo*, RelBatchInsertInfo*>(info.get());
    localState->nodeGroup = NodeGroupFactory::createNodeGroup(ColumnDataFormat::CSR,
        relInfo->columnTypes, relInfo->compressionEnabled);
}

void RelBatchInsert::executeInternal(ExecutionContext* context) {
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
        if (relTable->isNewNodeGroup(context->clientContext->getTx(), relLocalState->nodeGroupIdx,
                relInfo->direction)) {
            appendNewNodeGroup(*relInfo, *relLocalState, *sharedState, *partitionerSharedState);
        } else {
            mergeNodeGroup(context, *relInfo, *relLocalState, *sharedState,
                *partitionerSharedState);
        }
    }
}

void RelBatchInsert::prepareCSRNodeGroup(const ChunkedNodeGroupCollection& partition,
    offset_t startNodeOffset, const RelBatchInsertInfo& relInfo,
    RelBatchInsertLocalState& localState, offset_t numNodes) {
    auto csrNodeGroup =
        ku_dynamic_cast<ChunkedNodeGroup*, ChunkedCSRNodeGroup*>(localState.nodeGroup.get());
    auto& csrHeader = csrNodeGroup->getCSRHeader();
    csrHeader.setNumValues(numNodes);
    // Populate start csr offsets and lengths for each node.
    auto gaps =
        populateStartCSROffsetsAndLengths(csrHeader, numNodes, partition, relInfo.offsetColumnID);
    auto invalid = checkRelMultiplicityConstraint(csrHeader, relInfo);
    if (invalid.has_value()) {
        throw CopyException(ExceptionMessage::violateRelMultiplicityConstraint(
            relInfo.tableEntry->getName(), std::to_string(invalid.value() + startNodeOffset),
            RelDataDirectionUtils::relDirectionToString(relInfo.direction)));
    }
    // Resize csr data column chunks.
    offset_t csrChunkCapacity =
        csrHeader.getEndCSROffset(numNodes - 1) + csrHeader.getCSRLength(numNodes - 1);
    localState.nodeGroup->resizeChunks(csrChunkCapacity);
    localState.nodeGroup->setAllNull();
    for (auto& chunkedGroup : partition.getChunkedGroups()) {
        auto& offsetChunk = chunkedGroup->getColumnChunkUnsafe(relInfo.offsetColumnID);
        setOffsetFromCSROffsets(offsetChunk, *csrHeader.offset);
    }
    populateEndCSROffsets(csrHeader, gaps);
}

void RelBatchInsert::mergeNodeGroup(ExecutionContext* context, const RelBatchInsertInfo& relInfo,
    RelBatchInsertLocalState& localState, BatchInsertSharedState& sharedState,
    const PartitionerSharedState& partitionerSharedState) {
    auto relTable = ku_dynamic_cast<Table*, RelTable*>(sharedState.table);
    auto nodeGroupStartOffset = StorageUtils::getStartOffsetOfNodeGroup(localState.nodeGroupIdx);
    auto localNG = std::make_unique<LocalRelNG>(nodeGroupStartOffset, relInfo.columnTypes);
    auto& partition =
        partitionerSharedState.getPartitionBuffer(relInfo.partitioningIdx, localState.nodeGroupIdx);
    auto& insertChunks = localNG->getInsertChunks();
    auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(localState.nodeGroupIdx);
    auto numRels = 0u;
    for (auto& chunkedGroup : partition.getChunkedGroups()) {
        auto& offsetChunk = chunkedGroup->getColumnChunkUnsafe(relInfo.offsetColumnID);
        numRels += chunkedGroup->getNumRows();
        setOffsetToWithinNodeGroup(offsetChunk, startNodeOffset);
        std::vector<std::unique_ptr<ColumnChunk>> chunksToAppend;
        for (auto i = 0u; i < chunkedGroup->getNumColumns(); i++) {
            if (i == relInfo.offsetColumnID) {
                // Skip the offset vector.
                continue;
            }
            chunksToAppend.push_back(std::move(chunkedGroup->getColumnChunksUnsafe()[i]));
        }
        auto chunkedGroupToAppend = std::make_unique<ChunkedNodeGroup>(std::move(chunksToAppend));
        insertChunks.appendChunkedGroup(&offsetChunk, std::move(chunkedGroupToAppend));
    }
    relTable->getDirectedTableData(relInfo.direction)
        ->prepareCommitNodeGroup(context->clientContext->getTx(), localState.nodeGroupIdx,
            localNG.get());
    sharedState.incrementNumRows(numRels);
}

void RelBatchInsert::populateEndCSROffsets(ChunkedCSRHeader& csrHeader,
    std::vector<offset_t>& gaps) {
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
    offset_t numNodes, const ChunkedNodeGroupCollection& partition, column_id_t offsetColumnID) {
    KU_ASSERT(numNodes == csrHeader.length->getNumValues() &&
              numNodes == csrHeader.offset->getNumValues());
    std::vector<offset_t> gaps;
    gaps.resize(numNodes);
    auto csrOffsets = (offset_t*)csrHeader.offset->getData();
    auto csrLengths = (length_t*)csrHeader.length->getData();
    std::fill(csrLengths, csrLengths + numNodes, 0);
    // Calculate length for each node. Store the num of tuples of node i at csrLengths[i].
    for (auto& chunkedGroup : partition.getChunkedGroups()) {
        auto& offsetChunk = chunkedGroup->getColumnChunk(offsetColumnID);
        for (auto i = 0u; i < offsetChunk.getNumValues(); i++) {
            auto nodeOffset = offsetChunk.getValue<offset_t>(i);
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
    KU_ASSERT(chunk.getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
    auto offsets = (offset_t*)chunk.getData();
    for (auto i = 0u; i < chunk.getNumValues(); i++) {
        offsets[i] -= startOffset;
    }
}

void RelBatchInsert::setOffsetFromCSROffsets(ColumnChunk& nodeOffsetChunk,
    ColumnChunk& csrOffsetChunk) {
    KU_ASSERT(nodeOffsetChunk.getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
    for (auto i = 0u; i < nodeOffsetChunk.getNumValues(); i++) {
        auto nodeOffset = nodeOffsetChunk.getValue<offset_t>(i);
        auto csrOffset = csrOffsetChunk.getValue<offset_t>(nodeOffset);
        nodeOffsetChunk.setValue<offset_t>(csrOffset, i);
        csrOffsetChunk.setValue<offset_t>(csrOffset + 1, nodeOffset);
    }
}

void RelBatchInsert::appendNewNodeGroup(const RelBatchInsertInfo& relInfo,
    RelBatchInsertLocalState& localState, BatchInsertSharedState& sharedState,
    const PartitionerSharedState& partitionerSharedState) {
    auto nodeGroupIdx = localState.nodeGroupIdx;
    auto& partitioningBuffer =
        partitionerSharedState.getPartitionBuffer(relInfo.partitioningIdx, localState.nodeGroupIdx);
    auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    for (auto& chunkedGroup : partitioningBuffer.getChunkedGroups()) {
        setOffsetToWithinNodeGroup(chunkedGroup->getColumnChunkUnsafe(relInfo.offsetColumnID),
            startNodeOffset);
    }
    // Calculate num of source nodes in this node group.
    // This will be used to set the num of values of the node group.
    auto numNodes = std::min(StorageConstants::NODE_GROUP_SIZE,
        partitionerSharedState.maxNodeOffsets[relInfo.partitioningIdx] - startNodeOffset + 1);
    prepareCSRNodeGroup(partitioningBuffer, startNodeOffset, relInfo, localState, numNodes);
    for (auto& chunkedGroup : partitioningBuffer.getChunkedGroups()) {
        localState.nodeGroup->write(*chunkedGroup, relInfo.offsetColumnID);
    }
    localState.nodeGroup->finalize(nodeGroupIdx);
    // Flush node group to table.
    auto relTable = ku_dynamic_cast<Table*, RelTable*>(sharedState.table);
    relTable->append(localState.nodeGroup.get(), relInfo.direction);
    sharedState.incrementNumRows(localState.nodeGroup->getNumRows());
    localState.nodeGroup->resetToEmpty();
}

std::optional<offset_t> RelBatchInsert::checkRelMultiplicityConstraint(
    const storage::ChunkedCSRHeader& csrHeader, const RelBatchInsertInfo& relInfo) {
    auto relTableEntry =
        ku_dynamic_cast<catalog::TableCatalogEntry*, catalog::RelTableCatalogEntry*>(
            relInfo.tableEntry);
    if (!relTableEntry->isSingleMultiplicity(relInfo.direction)) {
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
        sharedState->updateNumTuplesForTable();
        auto outputMsg = stringFormat("{} tuples have been copied to the {} table.",
            sharedState->getNumRows(), info->tableEntry->getName());
        FactorizedTableUtils::appendStringToTable(sharedState->fTable.get(), outputMsg,
            context->clientContext->getMemoryManager());
    }
    sharedState->numRows.store(0);
    partitionerSharedState->resetState();
    partitionerSharedState->partitioningBuffers[relInfo->partitioningIdx].reset();
}

} // namespace processor
} // namespace kuzu
