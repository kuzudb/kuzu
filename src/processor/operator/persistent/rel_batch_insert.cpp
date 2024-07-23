#include "processor/operator/persistent/rel_batch_insert.h"

#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/string_format.h"
#include "processor/result/factorized_table_util.h"
#include "storage/storage_utils.h"
#include "storage/store/column_chunk_data.h"
#include "storage/store/rel_table.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void RelBatchInsert::initLocalStateInternal(ResultSet* /*resultSet_*/,
    ExecutionContext* /*context*/) {
    localState = std::make_unique<RelBatchInsertLocalState>();
    const auto relInfo = ku_dynamic_cast<BatchInsertInfo*, RelBatchInsertInfo*>(info.get());
    localState->chunkedGroup = std::make_unique<ChunkedCSRNodeGroup>(relInfo->columnTypes,
        relInfo->compressionEnabled, 0, 0, ResidencyState::IN_MEMORY);
    const auto nbrTableID =
        relInfo->tableEntry->constCast<RelTableCatalogEntry>().getNbrTableID(relInfo->direction);
    const auto relTableID = relInfo->tableEntry->getTableID();
    // TODO(Guodong): Get rid of the hard-coded nbr and rel column ID 0/1.
    localState->chunkedGroup->getColumnChunk(0).getData().cast<InternalIDChunkData>().setTableID(
        nbrTableID);
    localState->chunkedGroup->getColumnChunk(1).getData().cast<InternalIDChunkData>().setTableID(
        relTableID);
}

void RelBatchInsert::executeInternal(ExecutionContext* context) {
    auto relInfo = ku_dynamic_cast<BatchInsertInfo*, RelBatchInsertInfo*>(info.get());
    auto relTable = sharedState->table->ptrCast<RelTable>();
    auto relLocalState =
        ku_dynamic_cast<BatchInsertLocalState*, RelBatchInsertLocalState*>(localState.get());
    while (true) {
        relLocalState->nodeGroupIdx =
            partitionerSharedState->getNextPartition(relInfo->partitioningIdx);
        if (relLocalState->nodeGroupIdx == INVALID_PARTITION_IDX) {
            // No more partitions left in the partitioning buffer.
            break;
        }
        // TODO(Guodong): We need to handle the concurrency between COPY and other insertions into
        // the same node group.
        auto& nodeGroup =
            relTable->getOrCreateNodeGroup(relLocalState->nodeGroupIdx, relInfo->direction)
                ->cast<CSRNodeGroup>();
        appendNodeGroup(context->clientContext->getTx(), nodeGroup, *relInfo, *relLocalState,
            *sharedState, *partitionerSharedState);
    }
}

// TODO(Guodong): We should handle the case where bound/nbr nodes are created within the same
// transaction and not yet committed.
void RelBatchInsert::appendNodeGroup(transaction::Transaction* transaction, CSRNodeGroup& nodeGroup,
    const RelBatchInsertInfo& relInfo, RelBatchInsertLocalState& localState,
    BatchInsertSharedState& sharedState, const PartitionerSharedState& partitionerSharedState) {
    const auto nodeGroupIdx = localState.nodeGroupIdx;
    auto& partitioningBuffer =
        partitionerSharedState.getPartitionBuffer(relInfo.partitioningIdx, localState.nodeGroupIdx);
    const auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    for (auto& chunkedGroup : partitioningBuffer.getChunkedGroups()) {
        setOffsetToWithinNodeGroup(
            chunkedGroup->getColumnChunk(relInfo.boundNodeOffsetColumnID).getData(),
            startNodeOffset);
    }
    // Calculate num of source nodes in this node group.
    // This will be used to set the num of values of the node group.
    const auto numNodes = std::min(StorageConstants::NODE_GROUP_SIZE,
        partitionerSharedState.maxNodeOffsets[relInfo.partitioningIdx] - startNodeOffset + 1);
    const auto isNewNodeGroup = nodeGroup.isEmpty();
    // We optimistically flush new node group directly to disk in gapped CSR format.
    // There is no benefit of leaving gaps for existing node groups, which is kept in memory.
    const auto leaveGaps = isNewNodeGroup;
    populateCSROffsets(partitioningBuffer, startNodeOffset, relInfo, localState, numNodes,
        leaveGaps);
    for (auto& chunkedGroup : partitioningBuffer.getChunkedGroups()) {
        sharedState.incrementNumRows(chunkedGroup->getNumRows());
        localState.chunkedGroup->write(*chunkedGroup, relInfo.boundNodeOffsetColumnID);
    }
    localState.chunkedGroup->finalize();
    if (isNewNodeGroup) {
        auto flushedChunkedGroup = localState.chunkedGroup->flushAsNewChunkedNodeGroup(transaction,
            *sharedState.table->getDataFH());
        nodeGroup.setPersistentChunkedGroup(std::move(flushedChunkedGroup));
    } else {
        nodeGroup.appendChunkedCSRGroup(transaction,
            localState.chunkedGroup->cast<ChunkedCSRNodeGroup>());
    }
    localState.chunkedGroup->resetToEmpty();
}

void RelBatchInsert::populateCSROffsets(ChunkedNodeGroupCollection& partition,
    offset_t startNodeOffset, const RelBatchInsertInfo& relInfo,
    const RelBatchInsertLocalState& localState, offset_t numNodes, bool leaveGaps) {
    auto& csrNodeGroup = localState.chunkedGroup->cast<ChunkedCSRNodeGroup>();
    auto& csrHeader = csrNodeGroup.getCSRHeader();
    csrHeader.setNumValues(numNodes);
    // Populate start csr offsets and lengths for each node.
    populateCSRLengths(csrHeader, numNodes, partition, relInfo.boundNodeOffsetColumnID);
    const auto gaps = csrHeader.populateStartCSROffsetsAndGaps(leaveGaps);
    const auto invalid = checkRelMultiplicityConstraint(csrHeader, relInfo);
    if (invalid.has_value()) {
        throw CopyException(ExceptionMessage::violateRelMultiplicityConstraint(
            relInfo.tableEntry->getName(), std::to_string(invalid.value() + startNodeOffset),
            RelDataDirectionUtils::relDirectionToString(relInfo.direction)));
    }
    // Resize csr data column chunks.
    const offset_t csrChunkCapacity =
        csrHeader.getEndCSROffset(numNodes - 1) + csrHeader.getCSRLength(numNodes - 1);
    localState.chunkedGroup->resizeChunks(csrChunkCapacity);
    localState.chunkedGroup->resetToAllNull();
    for (auto& chunkedGroup : partition.getChunkedGroups()) {
        auto& offsetChunk = chunkedGroup->getColumnChunk(relInfo.boundNodeOffsetColumnID);
        setOffsetFromCSROffsets(offsetChunk.getData(), csrHeader.offset->getData());
    }
    csrHeader.populateEndCSROffsets(gaps);
    KU_ASSERT(csrHeader.sanityCheck());
}

void RelBatchInsert::populateCSRLengths(ChunkedCSRHeader& csrHeader, offset_t numNodes,
    ChunkedNodeGroupCollection& partition, column_id_t boundNodeOffsetColumn) {
    KU_ASSERT(numNodes == csrHeader.length->getNumValues() &&
              numNodes == csrHeader.offset->getNumValues());
    const auto lengthData = reinterpret_cast<length_t*>(csrHeader.length->getData().getData());
    std::fill(lengthData, lengthData + numNodes, 0);
    for (auto& chunkedGroup : partition.getChunkedGroups()) {
        auto& offsetChunk = chunkedGroup->getColumnChunk(boundNodeOffsetColumn);
        for (auto i = 0u; i < offsetChunk.getNumValues(); i++) {
            const auto nodeOffset = offsetChunk.getData().getValue<offset_t>(i);
            KU_ASSERT(nodeOffset < numNodes);
            lengthData[nodeOffset]++;
        }
    }
}

void RelBatchInsert::setOffsetToWithinNodeGroup(ColumnChunkData& chunk, offset_t startOffset) {
    KU_ASSERT(chunk.getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
    auto offsets = reinterpret_cast<offset_t*>(chunk.getData());
    for (auto i = 0u; i < chunk.getNumValues(); i++) {
        offsets[i] -= startOffset;
    }
}

void RelBatchInsert::setOffsetFromCSROffsets(ColumnChunkData& nodeOffsetChunk,
    ColumnChunkData& csrOffsetChunk) {
    KU_ASSERT(nodeOffsetChunk.getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
    for (auto i = 0u; i < nodeOffsetChunk.getNumValues(); i++) {
        auto nodeOffset = nodeOffsetChunk.getValue<offset_t>(i);
        auto csrOffset = csrOffsetChunk.getValue<offset_t>(nodeOffset);
        nodeOffsetChunk.setValue<offset_t>(csrOffset, i);
        csrOffsetChunk.setValue<offset_t>(csrOffset + 1, nodeOffset);
    }
}

std::optional<offset_t> RelBatchInsert::checkRelMultiplicityConstraint(
    const ChunkedCSRHeader& csrHeader, const RelBatchInsertInfo& relInfo) {
    auto& relTableEntry = relInfo.tableEntry->constCast<RelTableCatalogEntry>();
    if (!relTableEntry.isSingleMultiplicity(relInfo.direction)) {
        return std::nullopt;
    }
    for (auto i = 0u; i < csrHeader.length->getNumValues(); i++) {
        if (csrHeader.length->getData().getValue<length_t>(i) > 1) {
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
