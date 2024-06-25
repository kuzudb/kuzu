#include "storage/store/struct_column.h"

#include "storage/store/null_column.h"
#include "storage/store/struct_chunk_data.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

StructColumn::StructColumn(std::string name, LogicalType dataType, BMFileHandle* dataFH,
    BufferManager* bufferManager, WAL* wal, bool enableCompression)
    : Column{name, std::move(dataType), dataFH, bufferManager, wal, enableCompression,
          true /* requireNullColumn */} {
    const auto fieldTypes = StructType::getFieldTypes(this->dataType);
    childColumns.resize(fieldTypes.size());
    for (auto i = 0u; i < fieldTypes.size(); i++) {
        const auto childColName = StorageUtils::getColumnName(name,
            StorageUtils::ColumnType::STRUCT_CHILD, std::to_string(i));
        childColumns[i] = ColumnFactory::createColumn(childColName, fieldTypes[i]->copy(), dataFH,
            bufferManager, wal, enableCompression);
    }
}

std::unique_ptr<ColumnChunkData> StructColumn::flushChunkData(const ColumnChunkData& chunk,
    BMFileHandle& dataFH) {
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
    ColumnChunkData* columnChunk, offset_t startOffset, offset_t endOffset) {
    KU_ASSERT(columnChunk->getDataType().getPhysicalType() == PhysicalTypeID::STRUCT);
    nullColumn->scan(transaction, *state.nullState, columnChunk->getNullData(), startOffset,
        endOffset);
    auto numValues = state.metadata.numValues == 0 ?
                         0 :
                         std::min(endOffset, state.metadata.numValues) - startOffset;
    columnChunk->setNumValues(numValues);
    auto& structColumnChunk = columnChunk->cast<StructChunkData>();
    for (auto i = 0u; i < childColumns.size(); i++) {
        childColumns[i]->scan(transaction, state.childrenStates[i], structColumnChunk.getChild(i),
            startOffset, endOffset);
    }
}

void StructColumn::scan(Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    nullColumn->scan(transaction, *state.nullState, startOffsetInGroup, endOffsetInGroup,
        resultVector, offsetInVector);
    for (auto i = 0u; i < childColumns.size(); i++) {
        auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childColumns[i]->scan(transaction, state.childrenStates[i], startOffsetInGroup,
            endOffsetInGroup, fieldVector, offsetInVector);
    }
}

void StructColumn::scanInternal(Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInChunk, row_idx_t numValuesToScan, ValueVector* nodeIDVector,
    ValueVector* resultVector) {
    for (auto i = 0u; i < childColumns.size(); i++) {
        const auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childColumns[i]->scan(transaction, state.childrenStates[i], startOffsetInChunk,
            numValuesToScan, nodeIDVector, fieldVector);
    }
}

void StructColumn::lookupInternal(Transaction* transaction, ChunkState& readState,
    ValueVector* nodeIDVector, ValueVector* resultVector) {
    for (auto i = 0u; i < childColumns.size(); i++) {
        auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childColumns[i]->lookup(transaction, readState.childrenStates[i], nodeIDVector,
            fieldVector);
    }
}

void StructColumn::write(ChunkState& state, offset_t offsetInChunk, ValueVector* vectorToWriteFrom,
    uint32_t posInVectorToWriteFrom) {
    KU_ASSERT(vectorToWriteFrom->dataType.getPhysicalType() == PhysicalTypeID::STRUCT);
    if (vectorToWriteFrom->isNull(posInVectorToWriteFrom)) {
        return;
    }
    KU_ASSERT(childColumns.size() == StructVector::getFieldVectors(vectorToWriteFrom).size());
    for (auto i = 0u; i < childColumns.size(); i++) {
        auto fieldVector = StructVector::getFieldVector(vectorToWriteFrom, i).get();
        childColumns[i]->write(state.childrenStates[i], offsetInChunk, fieldVector,
            posInVectorToWriteFrom);
    }
}

void StructColumn::write(ChunkState& state, offset_t offsetInChunk, ColumnChunkData* data,
    offset_t dataOffset, length_t numValues) {
    KU_ASSERT(data->getDataType().getPhysicalType() == PhysicalTypeID::STRUCT);
    nullColumn->write(*state.nullState, offsetInChunk, data->getNullData(), dataOffset, numValues);
    auto& structData = data->cast<StructChunkData>();
    for (auto i = 0u; i < childColumns.size(); i++) {
        auto childData = structData.getChild(i);
        childColumns[i]->write(state.childrenStates[i], offsetInChunk, childData, dataOffset,
            numValues);
    }
}

void StructColumn::append(ColumnChunkData* columnChunk, ChunkState& state) {
    Column::append(columnChunk, state);
    KU_ASSERT(columnChunk->getDataType().getPhysicalType() == PhysicalTypeID::STRUCT);
    auto structColumnChunk = static_cast<StructChunkData*>(columnChunk);
    for (auto i = 0u; i < childColumns.size(); i++) {
        childColumns[i]->append(structColumnChunk->getChild(i), state.childrenStates[i]);
    }
}

// void StructColumn::prepareCommitForExistingChunk(Transaction* transaction, ChunkState& state,
//     const ChunkDataCollection& localInsertChunk, const offset_to_row_idx_t& insertInfo,
//     const ChunkDataCollection& localUpdateChunk, const offset_to_row_idx_t& updateInfo,
//     const offset_set_t& deleteInfo) {
//     // STRUCT column doesn't have actual data stored in buffer. Only need to update the null
//     // column.
//     nullColumn->prepareCommitForExistingChunk(transaction, *state.nullState,
//         getNullChunkCollection(localInsertChunk), insertInfo,
//         getNullChunkCollection(localUpdateChunk), updateInfo, deleteInfo);
//     // if (state.metadata.numValues != state.nullState->metadata.numValues) {
//     // state.metadata.numValues = state.nullState->metadata.numValues;
//     // metadataDA->update(state.nodeGroupIdx, state.metadata);
//     // }
//     // Update each child column separately
//     for (auto i = 0u; i < childColumns.size(); i++) {
//         childColumns[i]->prepareCommitForExistingChunk(transaction, state.childrenStates[i],
//             getStructChildChunkCollection(localInsertChunk, i), insertInfo,
//             getStructChildChunkCollection(localUpdateChunk, i), updateInfo, deleteInfo);
//     }
// }

void StructColumn::prepareCommitForExistingChunk(Transaction* transaction, ChunkState& state,
    const std::vector<offset_t>& dstOffsets, ColumnChunkData* chunk, offset_t srcOffset) {
    KU_ASSERT(chunk->getDataType().getPhysicalType() == dataType.getPhysicalType());
    // STRUCT column doesn't have actual data stored in buffer. Only need to update the null
    // column.
    nullColumn->prepareCommitForExistingChunk(transaction, *state.nullState, dstOffsets,
        chunk->getNullData(), srcOffset);
    // if (state.metadata.numValues != state.nullState->metadata.numValues) {
    // state.metadata.numValues = state.nullState->metadata.numValues;
    // metadataDA->update(state.nodeGroupIdx, state.metadata);
    // }
    // Update each child column separately
    for (auto i = 0u; i < childColumns.size(); i++) {
        const auto childChunk = chunk->cast<StructChunkData>().getChild(i);
        childColumns[i]->prepareCommitForExistingChunk(transaction, state.childrenStates[i],
            dstOffsets, childChunk, srcOffset);
    }
}

// ChunkDataCollection StructColumn::getStructChildChunkCollection(
//     const ChunkDataCollection& chunkCollection, idx_t childIdx) {
//     ChunkDataCollection childChunkCollection;
//     for (const auto& chunk : chunkCollection) {
//         auto& structChunk = chunk->cast<StructChunkData>();
//         childChunkCollection.push_back(structChunk.getChild(childIdx));
//     }
//     return childChunkCollection;
// }

} // namespace storage
} // namespace kuzu
