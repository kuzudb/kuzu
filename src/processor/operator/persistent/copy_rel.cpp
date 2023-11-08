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
    std::vector<std::unique_ptr<common::LogicalType>> columnTypes, RelsStoreStats* relsStatistics,
    MemoryManager* memoryManager)
    : tableID{tableID}, table{table}, columnTypes{std::move(columnTypes)},
      relsStatistics{relsStatistics}, numRows{0} {
    auto ftTableSchema = std::make_unique<FactorizedTableSchema>();
    ftTableSchema->appendColumn(
        std::make_unique<ColumnSchema>(false /* flat */, 0 /* dataChunkPos */,
            LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::STRING})));
    fTable = std::make_shared<FactorizedTable>(memoryManager, std::move(ftTableSchema));
}

void CopyRelSharedState::logCopyRelWALRecord(WAL* wal) {
    wal->logCopyTableRecord(tableID, TableType::REL);
    wal->flushAllPages();
}

void CopyRel::initLocalStateInternal(ResultSet* /*resultSet_*/, ExecutionContext* /*context*/) {
    localState = std::make_unique<CopyRelLocalState>();
    localState->nodeGroup = NodeGroupFactory::createNodeGroup(info->dataFormat,
        sharedState->columnTypes, info->compressionEnabled, true /* needFinalize */);
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
        auto partitioningBuffer = partitionerSharedState->partitioningBuffers[info->partitioningIdx]
                                      ->partitions[localState->currentPartition]
                                      .get();
        auto startOffset = StorageUtils::getStartOffsetOfNodeGroup(localState->currentPartition);
        auto offsetVectorIdx = info->dataDirection == RelDataDirection::FWD ? 0 : 1;
        row_idx_t numRows = 0;
        for (auto& dataChunk : *partitioningBuffer) {
            auto offsetVector = dataChunk->getValueVector(offsetVectorIdx).get();
            setOffsetToWithinNodeGroup(offsetVector, startOffset);
            numRows += offsetVector->state->selVector->selectedSize;
        }
        ColumnChunk* csrOffsetChunk = nullptr;
        if (info->dataFormat == ColumnDataFormat::CSR) {
            auto csrNodeGroup = static_cast<CSRNodeGroup*>(localState->nodeGroup.get());
            csrOffsetChunk = csrNodeGroup->getCSROffsetChunk();
            csrOffsetChunk->setNumValues(StorageConstants::NODE_GROUP_SIZE);
            populateCSROffsets(csrOffsetChunk, partitioningBuffer, offsetVectorIdx);
            // Resize csr data column chunks.
            localState->nodeGroup->resizeChunks(numRows);
        } else {
            // Set adj column chunk to all null.
            localState->nodeGroup->setChunkToAllNull(0 /* chunkIdx */);
        }
        for (auto& dataChunk : *partitioningBuffer) {
            if (info->dataFormat == ColumnDataFormat::CSR) {
                KU_ASSERT(csrOffsetChunk);
                auto offsetVector = dataChunk->getValueVector(offsetVectorIdx).get();
                setOffsetFromCSROffsets(offsetVector, (offset_t*)csrOffsetChunk->getData());
            }
            localState->nodeGroup->write(dataChunk.get(), offsetVectorIdx);
        }
        localState->nodeGroup->finalize(localState->currentPartition);
        // Flush node group to table.
        sharedState->table->append(localState->nodeGroup.get(), info->dataDirection);
        sharedState->incrementNumRows(localState->nodeGroup->getNumNodes());
        localState->nodeGroup->resetToEmpty();
    }
}

void CopyRel::populateCSROffsets(
    ColumnChunk* csrOffsetChunk, data_partition_t* partition, vector_idx_t offsetVectorIdx) {
    auto csrOffsets = (offset_t*)csrOffsetChunk->getData();
    std::fill(csrOffsets, csrOffsets + csrOffsetChunk->getCapacity(), 0);
    // Calculate num of tuples for each node. Store the num of tuples of node i at csrOffsets[i+1].
    for (auto& chunk : *partition) {
        auto offsetVector = chunk->getValueVector(offsetVectorIdx);
        for (auto i = 0u; i < offsetVector->state->selVector->selectedSize; i++) {
            auto pos = offsetVector->state->selVector->selectedPositions[i];
            auto nodeOffset = offsetVector->getValue<offset_t>(pos);
            if (nodeOffset >= StorageConstants::NODE_GROUP_SIZE - 1) {
                continue;
            }
            csrOffsets[nodeOffset + 1]++;
        }
    }
    // Calculate starting offset of each node.
    for (auto i = 1u; i < csrOffsetChunk->getCapacity(); i++) {
        csrOffsets[i] = csrOffsets[i] + csrOffsets[i - 1];
    }
}

// TODO(Guodong): Can we guarantee vector is not filtered and get rid of access to selVector?
void CopyRel::setOffsetToWithinNodeGroup(ValueVector* vector, offset_t startOffset) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::INT64);
    auto offsets = (offset_t*)vector->getData();
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        auto pos = vector->state->selVector->selectedPositions[i];
        offsets[pos] -= startOffset;
    }
}

void CopyRel::setOffsetFromCSROffsets(ValueVector* offsetVector, offset_t* csrOffsets) {
    KU_ASSERT(offsetVector->dataType.getPhysicalType() == PhysicalTypeID::INT64);
    for (auto i = 0u; i < offsetVector->state->selVector->selectedSize; i++) {
        auto pos = offsetVector->state->selVector->selectedPositions[i];
        auto nodeOffset = offsetVector->getValue<offset_t>(pos);
        offsetVector->setValue(pos, csrOffsets[nodeOffset]);
        csrOffsets[nodeOffset]++;
    }
}

void CopyRel::finalize(ExecutionContext* context) {
    if (info->partitioningIdx == 1) {
        sharedState->updateRelsStatistics();
        auto outputMsg = stringFormat("{} number of tuples has been copied to table {}.",
            sharedState->numRows.load(), info->schema->tableName);
        FactorizedTableUtils::appendStringToTable(
            sharedState->fTable.get(), outputMsg, context->memoryManager);
    }
    sharedState->numRows = 0;
    partitionerSharedState->resetState();
    partitionerSharedState->partitioningBuffers[info->partitioningIdx].reset();
}

} // namespace processor
} // namespace kuzu
