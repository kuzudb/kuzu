#include "storage/store/struct_column.h"

#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/storage_utils.h"
#include "storage/store/column_chunk.h"
#include "storage/store/null_column.h"
#include "storage/store/struct_chunk_data.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

StructColumn::StructColumn(std::string name, LogicalType dataType, FileHandle* dataFH,
    MemoryManager* mm, ShadowFile* shadowFile, bool enableCompression)
    : Column{std::move(name), std::move(dataType), dataFH, mm, shadowFile, enableCompression,
          true /* requireNullColumn */} {
    const auto fieldTypes = StructType::getFieldTypes(this->dataType);
    childColumns.resize(fieldTypes.size());
    for (auto i = 0u; i < fieldTypes.size(); i++) {
        const auto childColName = StorageUtils::getColumnName(this->name,
            StorageUtils::ColumnType::STRUCT_CHILD, std::to_string(i));
        childColumns[i] = ColumnFactory::createColumn(childColName, fieldTypes[i]->copy(), dataFH,
            mm, shadowFile, enableCompression);
    }
}

std::unique_ptr<ColumnChunkData> StructColumn::flushChunkData(const ColumnChunkData& chunk,
    FileHandle& dataFH) {
    auto flushedChunk = flushNonNestedChunkData(chunk, dataFH);
    auto& structChunk = chunk.cast<StructChunkData>();
    auto& flushedStructChunk = flushedChunk->cast<StructChunkData>();
    for (auto i = 0u; i < structChunk.getNumChildren(); i++) {
        auto flushedChildChunk = Column::flushChunkData(structChunk.getChild(i), dataFH);
        flushedStructChunk.setChild(i, std::move(flushedChildChunk));
    }
    return flushedChunk;
}

void StructColumn::scanInternal(const transaction::Transaction* transaction,
    const SegmentState& state, common::offset_t startOffsetInSegment,
    common::row_idx_t numValuesToScan, ColumnChunkData* resultChunk,
    common::offset_t offsetInResult) const {
    KU_ASSERT(resultChunk->getDataType().getPhysicalType() == PhysicalTypeID::STRUCT);
    Column::scanInternal(transaction, state, startOffsetInSegment, numValuesToScan, resultChunk,
        offsetInResult);
    auto& structColumnChunk = resultChunk->cast<StructChunkData>();
    for (auto i = 0u; i < childColumns.size(); i++) {
        childColumns[i]->scanInternal(transaction, state.childrenStates[i], startOffsetInSegment,
            numValuesToScan, structColumnChunk.getChild(i), offsetInResult);
    }
}

void StructColumn::scanInternal(const Transaction* transaction, const SegmentState& state,
    offset_t startOffsetInSegment, row_idx_t numValuesToScan, ValueVector* resultVector,
    offset_t offsetInResult) const {
    for (auto i = 0u; i < childColumns.size(); i++) {
        const auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childColumns[i]->scanInternal(transaction, state.childrenStates[i], startOffsetInSegment,
            numValuesToScan, fieldVector, offsetInResult);
    }
}

void StructColumn::lookupInternal(const Transaction* transaction, const SegmentState& state,
    offset_t offsetInSegment, ValueVector* resultVector, uint32_t posInVector) const {
    for (auto i = 0u; i < childColumns.size(); i++) {
        const auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childColumns[i]->lookupInternal(transaction, state.childrenStates[i], offsetInSegment,
            fieldVector, posInVector);
    }
}

void StructColumn::writeInternal(ColumnChunkData& persistentChunk, SegmentState& state,
    offset_t offsetInSegment, const ColumnChunkData& data, offset_t dataOffset,
    length_t numValues) const {
    KU_ASSERT(data.getDataType().getPhysicalType() == PhysicalTypeID::STRUCT);
    nullColumn->writeInternal(*persistentChunk.getNullData(), *state.nullState, offsetInSegment,
        *data.getNullData(), dataOffset, numValues);
    auto& structData = data.cast<StructChunkData>();
    auto& persistentStructChunk = persistentChunk.cast<StructChunkData>();
    for (auto i = 0u; i < childColumns.size(); i++) {
        const auto& childData = structData.getChild(i);
        childColumns[i]->writeInternal(*persistentStructChunk.getChild(i), state.childrenStates[i],
            offsetInSegment, childData, dataOffset, numValues);
    }
}

void StructColumn::checkpointSegment(ColumnCheckpointState&& checkpointState) const {
    auto& persistentStructChunk = checkpointState.persistentData.cast<StructChunkData>();
    for (auto i = 0u; i < childColumns.size(); i++) {
        std::vector<SegmentCheckpointState> childSegmentCheckpointStates;
        for (const auto& segmentCheckpointState : checkpointState.segmentCheckpointStates) {
            childSegmentCheckpointStates.emplace_back(
                segmentCheckpointState.chunkData.cast<StructChunkData>().getChild(i),
                segmentCheckpointState.startRowInData, segmentCheckpointState.offsetInSegment,
                segmentCheckpointState.numRows);
        }
        childColumns[i]->checkpointSegment(ColumnCheckpointState(*persistentStructChunk.getChild(i),
            std::move(childSegmentCheckpointStates)));
    }
    Column::checkpointNullData(checkpointState);
    persistentStructChunk.syncNumValues();
}

} // namespace storage
} // namespace kuzu
