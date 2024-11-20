#include "processor/operator/persistent/rel_batch_insert.h"

#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/string_format.h"
#include "common/task_system/progress_bar.h"
#include "processor/result/factorized_table_util.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/buffer_manager/spiller.h"
#include "storage/storage_utils.h"
#include "storage/store/column_chunk_data.h"
#include "storage/store/rel_table.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::string RelBatchInsertPrintInfo::toString() const {
    std::string result = "Table Name: ";
    result += tableName;
    return result;
}

void RelBatchInsert::initLocalStateInternal(ResultSet* /*resultSet_*/, ExecutionContext* context) {
    localState = std::make_unique<RelBatchInsertLocalState>();
    const auto relInfo = info->ptrCast<RelBatchInsertInfo>();
    localState->chunkedGroup =
        std::make_unique<ChunkedCSRNodeGroup>(*context->clientContext->getMemoryManager(),
            relInfo->columnTypes, relInfo->compressionEnabled, 0, 0, ResidencyState::IN_MEMORY);
    const auto nbrTableID =
        relInfo->tableEntry->constCast<RelTableCatalogEntry>().getNbrTableID(relInfo->direction);
    const auto relTableID = relInfo->tableEntry->getTableID();
    // TODO(Guodong): Get rid of the hard-coded nbr and rel column ID 0/1.
    localState->chunkedGroup->getColumnChunk(0).getData().cast<InternalIDChunkData>().setTableID(
        nbrTableID);
    localState->chunkedGroup->getColumnChunk(1).getData().cast<InternalIDChunkData>().setTableID(
        relTableID);
    const auto relLocalState = localState->ptrCast<RelBatchInsertLocalState>();
    relLocalState->dummyAllNullDataChunk = std::make_unique<DataChunk>(relInfo->columnTypes.size());
    for (auto i = 0u; i < relInfo->columnTypes.size(); i++) {
        auto valueVector = std::make_shared<ValueVector>(relInfo->columnTypes[i].copy(),
            context->clientContext->getMemoryManager());
        valueVector->setAllNull();
        relLocalState->dummyAllNullDataChunk->insert(i, std::move(valueVector));
    }
}

void RelBatchInsert::initGlobalStateInternal(ExecutionContext* /* context */) {
    progressSharedState = std::make_shared<RelBatchInsertProgressSharedState>();
    progressSharedState->partitionsDone = 0;
    progressSharedState->partitionsTotal =
        partitionerSharedState->numPartitions[info->ptrCast<RelBatchInsertInfo>()->partitioningIdx];
}

void RelBatchInsert::executeInternal(ExecutionContext* context) {
    const auto relInfo = info->ptrCast<RelBatchInsertInfo>();
    const auto relTable = sharedState->table->ptrCast<RelTable>();
    const auto relLocalState = localState->ptrCast<RelBatchInsertLocalState>();
    while (true) {
        relLocalState->nodeGroupIdx = partitionerSharedState->getNextPartition(
            relInfo->partitioningIdx, *progressSharedState);
        if (relLocalState->nodeGroupIdx == INVALID_PARTITION_IDX) {
            // No more partitions left in the partitioning buffer.
            break;
        }
        // TODO(Guodong): We need to handle the concurrency between COPY and other insertions
        // into the same node group.
        auto& nodeGroup = relTable
                              ->getOrCreateNodeGroup(context->clientContext->getTx(),
                                  relLocalState->nodeGroupIdx, relInfo->direction)
                              ->cast<CSRNodeGroup>();
        appendNodeGroup(context->clientContext->getTx(), nodeGroup, *relInfo, *relLocalState,
            *sharedState, *partitionerSharedState);
        updateProgress(context);
    }
}

static void appendNewChunkedGroup(transaction::Transaction* transaction,
    ChunkedCSRNodeGroup& chunkedGroup, RelTable& relTable, CSRNodeGroup& nodeGroup,
    RelDataDirection direction) {
    const bool isNewNodeGroup = nodeGroup.isEmpty();
    const CSRNodeGroupScanSource source = isNewNodeGroup ?
                                              CSRNodeGroupScanSource::COMMITTED_PERSISTENT :
                                              CSRNodeGroupScanSource::COMMITTED_IN_MEMORY;
    // since each thread operates on distinct node groups
    // We don't need a lock here (to ensure the insert info and append agree on the number of rows
    // in the node group)
    relTable.pushInsertInfo(transaction, direction, nodeGroup, chunkedGroup.getNumRows(), source);
    if (isNewNodeGroup) {
        auto flushedChunkedGroup =
            chunkedGroup.flushAsNewChunkedNodeGroup(transaction, *relTable.getDataFH());
        nodeGroup.setPersistentChunkedGroup(std::move(flushedChunkedGroup));
    } else {
        nodeGroup.appendChunkedCSRGroup(transaction, chunkedGroup);
    }
}

void RelBatchInsert::appendNodeGroup(transaction::Transaction* transaction, CSRNodeGroup& nodeGroup,
    const RelBatchInsertInfo& relInfo, const RelBatchInsertLocalState& localState,
    BatchInsertSharedState& sharedState, const PartitionerSharedState& partitionerSharedState) {
    const auto nodeGroupIdx = localState.nodeGroupIdx;
    auto partitioningBuffer =
        partitionerSharedState.getPartitionBuffer(relInfo.partitioningIdx, localState.nodeGroupIdx);
    const auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    for (auto& chunkedGroup : partitioningBuffer->getChunkedGroups()) {
        setOffsetToWithinNodeGroup(
            chunkedGroup->getColumnChunk(relInfo.boundNodeOffsetColumnID).getData(),
            startNodeOffset);
    }
    // Calculate num of source nodes in this node group.
    // This will be used to set the num of values of the node group.
    const auto numNodes = std::min(StorageConstants::NODE_GROUP_SIZE,
        partitionerSharedState.maxNodeOffsets[relInfo.partitioningIdx] - startNodeOffset + 1);
    // We optimistically flush new node group directly to disk in gapped CSR format.
    // There is no benefit of leaving gaps for existing node groups, which is kept in memory.
    const auto leaveGaps = nodeGroup.isEmpty();
    populateCSRHeaderAndRowIdx(*partitioningBuffer, startNodeOffset, relInfo, localState, numNodes,
        leaveGaps);
    const auto& csrHeader = localState.chunkedGroup->cast<ChunkedCSRNodeGroup>().getCSRHeader();
    const auto maxSize = csrHeader.getEndCSROffset(numNodes - 1);
    for (auto& chunkedGroup : partitioningBuffer->getChunkedGroups()) {
        sharedState.incrementNumRows(chunkedGroup->getNumRows());
        localState.chunkedGroup->write(*chunkedGroup, relInfo.boundNodeOffsetColumnID);
    }
    // Reset num of rows in the chunked group to fill gaps at the end of the node group.
    auto numGapsAtEnd = maxSize - localState.chunkedGroup->getNumRows();
    KU_ASSERT(localState.chunkedGroup->getCapacity() >= maxSize);
    while (numGapsAtEnd > 0) {
        const auto numGapsToFill = std::min(numGapsAtEnd, DEFAULT_VECTOR_CAPACITY);
        localState.dummyAllNullDataChunk->state->getSelVectorUnsafe().setSelSize(numGapsToFill);
        std::vector<ValueVector*> dummyVectors;
        for (auto i = 0u; i < relInfo.columnTypes.size(); i++) {
            dummyVectors.push_back(&localState.dummyAllNullDataChunk->getValueVectorMutable(i));
        }
        const auto numGapsFilled = localState.chunkedGroup->append(&transaction::DUMMY_TRANSACTION,
            dummyVectors, 0, numGapsToFill);
        KU_ASSERT(numGapsFilled == numGapsToFill);
        numGapsAtEnd -= numGapsFilled;
    }
    KU_ASSERT(localState.chunkedGroup->getNumRows() == maxSize);
    localState.chunkedGroup->finalize();

    auto* relTable = sharedState.table->ptrCast<RelTable>();
    appendNewChunkedGroup(transaction, localState.chunkedGroup->cast<ChunkedCSRNodeGroup>(),
        *relTable, nodeGroup, relInfo.direction);

    localState.chunkedGroup->resetToEmpty();
}

void RelBatchInsert::populateCSRHeaderAndRowIdx(InMemChunkedNodeGroupCollection& partition,
    offset_t startNodeOffset, const RelBatchInsertInfo& relInfo,
    const RelBatchInsertLocalState& localState, offset_t numNodes, bool leaveGaps) {
    auto& csrNodeGroup = localState.chunkedGroup->cast<ChunkedCSRNodeGroup>();
    auto& csrHeader = csrNodeGroup.getCSRHeader();
    csrHeader.setNumValues(numNodes);
    // Populate lengths for each node and check multiplicity constraint.
    populateCSRLengths(csrHeader, numNodes, partition, relInfo.boundNodeOffsetColumnID);
    checkRelMultiplicityConstraint(csrHeader, startNodeOffset, relInfo);
    const auto rightCSROffsetOfRegions = csrHeader.populateStartCSROffsetsFromLength(leaveGaps);
    // Resize csr data column chunks.
    const auto csrChunkCapacity = rightCSROffsetOfRegions.back() + 1;
    localState.chunkedGroup->resizeChunks(csrChunkCapacity);
    localState.chunkedGroup->resetToAllNull();
    for (auto& chunkedGroup : partition.getChunkedGroups()) {
        auto& offsetChunk = chunkedGroup->getColumnChunk(relInfo.boundNodeOffsetColumnID);
        // We reuse bound node offset column to store row idx for each rel in the node group.
        setRowIdxFromCSROffsets(offsetChunk.getData(), csrHeader.offset->getData());
    }
    csrHeader.finalizeCSRRegionEndOffsets(rightCSROffsetOfRegions);
    KU_ASSERT(csrHeader.sanityCheck());
}

void RelBatchInsert::populateCSRLengths(const ChunkedCSRHeader& csrHeader, offset_t numNodes,
    InMemChunkedNodeGroupCollection& partition, column_id_t boundNodeOffsetColumn) {
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
    const auto offsets = reinterpret_cast<offset_t*>(chunk.getData());
    for (auto i = 0u; i < chunk.getNumValues(); i++) {
        offsets[i] -= startOffset;
    }
}

void RelBatchInsert::setRowIdxFromCSROffsets(ColumnChunkData& rowIdxChunk,
    ColumnChunkData& csrOffsetChunk) {
    KU_ASSERT(rowIdxChunk.getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
    for (auto i = 0u; i < rowIdxChunk.getNumValues(); i++) {
        const auto nodeOffset = rowIdxChunk.getValue<offset_t>(i);
        const auto csrOffset = csrOffsetChunk.getValue<offset_t>(nodeOffset);
        rowIdxChunk.setValue<offset_t>(csrOffset, i);
        // Increment current csr offset for nodeOffset by 1.
        csrOffsetChunk.setValue<offset_t>(csrOffset + 1, nodeOffset);
    }
}

void RelBatchInsert::checkRelMultiplicityConstraint(const ChunkedCSRHeader& csrHeader,
    offset_t startNodeOffset, const RelBatchInsertInfo& relInfo) {
    auto& relTableEntry = relInfo.tableEntry->constCast<RelTableCatalogEntry>();
    if (!relTableEntry.isSingleMultiplicity(relInfo.direction)) {
        return;
    }
    for (auto i = 0u; i < csrHeader.length->getNumValues(); i++) {
        if (csrHeader.length->getData().getValue<length_t>(i) > 1) {
            throw CopyException(ExceptionMessage::violateRelMultiplicityConstraint(
                relInfo.tableEntry->getName(), std::to_string(i + startNodeOffset),
                RelDataDirectionUtils::relDirectionToString(relInfo.direction)));
        }
    }
}

void RelBatchInsert::finalizeInternal(ExecutionContext* context) {
    const auto relInfo = info->ptrCast<RelBatchInsertInfo>();
    if (relInfo->direction == RelDataDirection::BWD) {
        KU_ASSERT(
            relInfo->partitioningIdx == partitionerSharedState->partitioningBuffers.size() - 1);

        auto outputMsg = stringFormat("{} tuples have been copied to the {} table.",
            sharedState->getNumRows(), info->tableEntry->getName());
        FactorizedTableUtils::appendStringToTable(sharedState->fTable.get(), outputMsg,
            context->clientContext->getMemoryManager());

        const auto warningCount =
            context->clientContext->getWarningContextUnsafe().getWarningCount(context->queryID);
        if (warningCount > 0) {
            auto warningMsg =
                stringFormat("{} warnings encountered during copy. Use 'CALL "
                             "show_warnings() RETURN *' to view the actual warnings. Query ID: {}",
                    warningCount, context->queryID);
            FactorizedTableUtils::appendStringToTable(sharedState->fTable.get(), warningMsg,
                context->clientContext->getMemoryManager());
        }
    }
    sharedState->numRows.store(0);
    sharedState->table->cast<RelTable>().setHasChanges();
    partitionerSharedState->resetState();
    partitionerSharedState->partitioningBuffers[relInfo->partitioningIdx].reset();
}

void RelBatchInsert::updateProgress(ExecutionContext* context) {
    if (progressSharedState->partitionsTotal == 0) {
        context->clientContext->getProgressBar()->updateProgress(context->queryID, 0);
    } else {
        double progress = double(progressSharedState->partitionsDone) /
                          double(progressSharedState->partitionsTotal);
        context->clientContext->getProgressBar()->updateProgress(context->queryID, progress);
    }
}

} // namespace processor
} // namespace kuzu
