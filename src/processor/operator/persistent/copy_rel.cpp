#include "processor/operator/persistent/copy_rel.h"

#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/string_format.h"
#include "processor/result/factorized_table.h"
#include "storage/store/rel_table.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

CopyRelSharedState::CopyRelSharedState(table_id_t tableID, RelTable* table,
    std::vector<std::unique_ptr<LogicalType>> columnTypes, RelsStoreStats* relsStatistics,
    MemoryManager* memoryManager)
    : tableID{tableID}, table{table}, columnTypes{std::move(columnTypes)},
      relsStatistics{relsStatistics}, numRows{0} {
    auto ftTableSchema = std::make_unique<FactorizedTableSchema>();
    ftTableSchema->appendColumn(
        std::make_unique<ColumnSchema>(false /* flat */, 0 /* dataChunkPos */,
            LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::STRING})));
    fTable = std::make_shared<FactorizedTable>(memoryManager, std::move(ftTableSchema));
}

// NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
void CopyRelSharedState::logCopyRelWALRecord(WAL* wal) {
    wal->logCopyTableRecord(tableID, TableType::REL);
    wal->flushAllPages();
}

void CopyRel::initLocalStateInternal(ResultSet* /*resultSet_*/, ExecutionContext* /*context*/) {
    localState = std::make_unique<CopyRelLocalState>();
    localState->nodeGroup = NodeGroupFactory::createNodeGroup(
        info->dataFormat, sharedState->columnTypes, info->compressionEnabled);
}

void CopyRel::initGlobalStateInternal(ExecutionContext* /*context*/) {
    if (!isCopyAllowed()) {
        throw CopyException(ExceptionMessage::notAllowCopyOnNonEmptyTableException());
    }
    sharedState->logCopyRelWALRecord(info->wal);
}

void CopyRel::executeInternal(ExecutionContext* /*context*/) {
    while (true) {
        localState->currentPartition =
            partitionerSharedState->getNextPartition(info->partitioningIdx);
        if (localState->currentPartition == INVALID_PARTITION_IDX) {
            break;
        }
        // Read the whole partition, and set to node group.
        auto partitioningBuffer = partitionerSharedState->getPartitionBuffer(
            info->partitioningIdx, localState->currentPartition);
        auto startOffset = StorageUtils::getStartOffsetOfNodeGroup(localState->currentPartition);
        auto offsetVectorIdx = info->dataDirection == RelDataDirection::FWD ? 0 : 1;
        for (auto dataChunk : partitioningBuffer->getChunks()) {
            setOffsetToWithinNodeGroup(
                dataChunk->getValueVector(offsetVectorIdx).get(), startOffset);
        }
        // Calculate num of source nodes in this node group.
        // This will be used to set the num of values of the node group.
        auto numNodes = std::min(StorageConstants::NODE_GROUP_SIZE,
            partitionerSharedState->maxNodeOffsets[info->partitioningIdx] - startOffset + 1);
        if (info->dataFormat == ColumnDataFormat::CSR) {
            prepareCSRNodeGroup(partitioningBuffer, offsetVectorIdx, numNodes);
        } else {
            localState->nodeGroup->setAllNull();
            localState->nodeGroup->getColumnChunk(0)->setNumValues(numNodes);
        }
        for (auto dataChunk : partitioningBuffer->getChunks()) {
            localState->nodeGroup->write(dataChunk, offsetVectorIdx);
        }
        localState->nodeGroup->finalize(localState->currentPartition);
        // Flush node group to table.
        sharedState->table->append(localState->nodeGroup.get(), info->dataDirection);
        sharedState->incrementNumRows(localState->nodeGroup->getNumRows());
        localState->nodeGroup->resetToEmpty();
    }
}

void CopyRel::prepareCSRNodeGroup(
    DataChunkCollection* partition, vector_idx_t offsetVectorIdx, offset_t numNodes) {
    auto csrNodeGroup = ku_dynamic_cast<NodeGroup*, CSRNodeGroup*>(localState->nodeGroup.get());
    auto& csrHeader = csrNodeGroup->getCSRHeader();
    csrHeader.setNumValues(numNodes);
    std::vector<length_t> gaps;
    gaps.resize(numNodes);
    // Populate start csr offsets and lengths for each node.
    populateStartCSROffsetsAndLengths(csrHeader, gaps, numNodes, partition, offsetVectorIdx);
    // Resize csr data column chunks.
    offset_t csrChunkCapacity =
        csrHeader.getEndCSROffset(numNodes - 1) + csrHeader.getCSRLength(numNodes - 1);
    localState->nodeGroup->resizeChunks(csrChunkCapacity);
    for (auto dataChunk : partition->getChunks()) {
        auto offsetVector = dataChunk->getValueVector(offsetVectorIdx).get();
        setOffsetFromCSROffsets(offsetVector, csrHeader.offset.get());
    }
    populateEndCSROffsets(csrHeader, gaps);
}

void CopyRel::populateEndCSROffsets(CSRHeaderChunks& csrHeader, std::vector<offset_t>& gaps) {
    auto csrOffsets = (offset_t*)csrHeader.offset->getData();
    for (auto i = 0u; i < csrHeader.offset->getNumValues(); i++) {
        csrOffsets[i] += gaps[i];
    }
}

void CopyRel::populateStartCSROffsetsAndLengths(CSRHeaderChunks& csrHeader,
    std::vector<length_t>& gaps, offset_t numNodes, DataChunkCollection* partition,
    vector_idx_t offsetVectorIdx) {
    KU_ASSERT(numNodes == csrHeader.length->getNumValues() &&
              numNodes == csrHeader.offset->getNumValues());
    auto csrOffsets = (offset_t*)csrHeader.offset->getData();
    auto csrLengths = (length_t*)csrHeader.length->getData();
    std::fill(csrLengths, csrLengths + numNodes, 0);
    // Calculate length for each node. Store the num of tuples of node i at csrLengths[i].
    for (auto chunk : partition->getChunks()) {
        auto offsetVector = chunk->getValueVector(offsetVectorIdx);
        for (auto i = 0u; i < offsetVector->state->selVector->selectedSize; i++) {
            auto pos = offsetVector->state->selVector->selectedPositions[i];
            auto nodeOffset = offsetVector->getValue<offset_t>(pos);
            KU_ASSERT(nodeOffset < numNodes);
            csrLengths[nodeOffset]++;
        }
    }
    // Calculate gaps for each node.
    for (auto i = 0u; i < numNodes; i++) {
        auto lengthWithGap = static_cast<length_t>(
            std::ceil((double)csrLengths[i] / (double)StorageConstants::PACKED_CSR_DENSITY));
        gaps[i] = lengthWithGap - csrLengths[i];
    }
    csrOffsets[0] = 0;
    // Calculate starting offset of each node.
    for (auto i = 1u; i < numNodes; i++) {
        csrOffsets[i] = csrOffsets[i - 1] + csrLengths[i - 1] + gaps[i - 1];
    }
}

void CopyRel::setOffsetToWithinNodeGroup(ValueVector* vector, offset_t startOffset) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::INT64 &&
              vector->state->selVector->isUnfiltered());
    auto offsets = (offset_t*)vector->getData();
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        offsets[i] -= startOffset;
    }
}

void CopyRel::setOffsetFromCSROffsets(ValueVector* offsetVector, ColumnChunk* offsetChunk) {
    KU_ASSERT(offsetVector->dataType.getPhysicalType() == PhysicalTypeID::INT64 &&
              offsetVector->state->selVector->isUnfiltered());
    for (auto i = 0u; i < offsetVector->state->selVector->selectedSize; i++) {
        auto nodeOffset = offsetVector->getValue<offset_t>(i);
        auto csrOffset = offsetChunk->getValue<offset_t>(nodeOffset);
        offsetVector->setValue<offset_t>(i, csrOffset);
        offsetChunk->setValue<offset_t>(csrOffset + 1, nodeOffset);
    }
}

void CopyRel::finalize(ExecutionContext* context) {
    if (info->partitioningIdx == partitionerSharedState->partitioningBuffers.size() - 1) {
        sharedState->updateRelsStatistics();
        auto outputMsg = stringFormat("{} number of tuples has been copied to table {}.",
            sharedState->numRows.load(), info->relTableEntry->getName());
        FactorizedTableUtils::appendStringToTable(
            sharedState->fTable.get(), outputMsg, context->clientContext->getMemoryManager());
    }
    sharedState->numRows.store(0);
    partitionerSharedState->resetState();
    partitionerSharedState->partitioningBuffers[info->partitioningIdx].reset();
}

} // namespace processor
} // namespace kuzu
