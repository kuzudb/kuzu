#include "storage/store/struct_column.h"

#include "common/vector/value_vector.h"
#include "storage/buffer_manager/memory_manager.h"
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

void StructColumn::scan(Transaction* transaction, const ChunkState& state,
    ColumnChunkData* columnChunk, offset_t startOffset, offset_t endOffset) const {
    KU_ASSERT(columnChunk->getDataType().getPhysicalType() == PhysicalTypeID::STRUCT);
    Column::scan(transaction, state, columnChunk, startOffset, endOffset);
    auto& structColumnChunk = columnChunk->cast<StructChunkData>();
    for (auto i = 0u; i < childColumns.size(); i++) {
        childColumns[i]->scan(transaction, state.childrenStates[i], structColumnChunk.getChild(i),
            startOffset, endOffset);
    }
}

void StructColumn::scan(Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) const {
    nullColumn->scan(transaction, *state.nullState, startOffsetInGroup, endOffsetInGroup,
        resultVector, offsetInVector);
    for (auto i = 0u; i < childColumns.size(); i++) {
        const auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childColumns[i]->scan(transaction, state.childrenStates[i], startOffsetInGroup,
            endOffsetInGroup, fieldVector, offsetInVector);
    }
}

void StructColumn::scanInternal(Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInChunk, row_idx_t numValuesToScan, ValueVector* resultVector) const {
    for (auto i = 0u; i < childColumns.size(); i++) {
        const auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childColumns[i]->scan(transaction, state.childrenStates[i], startOffsetInChunk,
            numValuesToScan, fieldVector);
    }
}

void StructColumn::lookupInternal(Transaction* transaction, const ChunkState& state,
    offset_t nodeOffset, ValueVector* resultVector, uint32_t posInVector) const {
    for (auto i = 0u; i < childColumns.size(); i++) {
        const auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childColumns[i]->lookupValue(transaction, state.childrenStates[i], nodeOffset, fieldVector,
            posInVector);
    }
}

void StructColumn::write(ColumnChunkData& persistentChunk, ChunkState& state,
    offset_t offsetInChunk, ColumnChunkData* data, offset_t dataOffset, length_t numValues) {
    KU_ASSERT(data->getDataType().getPhysicalType() == PhysicalTypeID::STRUCT);
    nullColumn->write(*persistentChunk.getNullData(), *state.nullState, offsetInChunk,
        data->getNullData(), dataOffset, numValues);
    auto& structData = data->cast<StructChunkData>();
    auto& persistentStructChunk = persistentChunk.cast<StructChunkData>();
    for (auto i = 0u; i < childColumns.size(); i++) {
        const auto childData = structData.getChild(i);
        childColumns[i]->write(*persistentStructChunk.getChild(i), state.childrenStates[i],
            offsetInChunk, childData, dataOffset, numValues);
    }
}

void StructColumn::checkpointColumnChunk(ColumnCheckpointState& checkpointState) {
    auto& persistentStructChunk = checkpointState.persistentData.cast<StructChunkData>();
    for (auto i = 0u; i < childColumns.size(); i++) {
        std::vector<ChunkCheckpointState> childChunkCheckpointStates;
        for (const auto& chunkCheckpointState : checkpointState.chunkCheckpointStates) {
            ChunkCheckpointState childChunkCheckpointState(
                chunkCheckpointState.chunkData->cast<StructChunkData>().moveChild(i),
                chunkCheckpointState.startRow, chunkCheckpointState.numRows);
            childChunkCheckpointStates.push_back(std::move(childChunkCheckpointState));
        }
        ColumnCheckpointState childColumnCheckpointState(*persistentStructChunk.getChild(i),
            std::move(childChunkCheckpointStates));
        childColumns[i]->checkpointColumnChunk(childColumnCheckpointState);
    }
    Column::checkpointNullData(checkpointState);
    persistentStructChunk.syncNumValues();
}

} // namespace storage
} // namespace kuzu
