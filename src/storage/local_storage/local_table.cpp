#include "storage/local_storage/local_table.h"

#include "storage/local_storage/local_node_table.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/store/column.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

LocalVectorCollection LocalVectorCollection::getStructChildVectorCollection(
    struct_field_idx_t idx) const {
    LocalVectorCollection childCollection;
    for (auto vector : vectors) {
        auto fieldVector = StructVector::getFieldVector(vector, idx).get();
        childCollection.vectors.push_back(fieldVector);
    }
    return childCollection;
}

LocalNodeGroup::LocalNodeGroup(
    offset_t nodeGroupStartOffset, std::vector<LogicalType*> dataTypes, MemoryManager* mm)
    : nodeGroupStartOffset{nodeGroupStartOffset}, insertChunks{mm, LogicalType::copy(dataTypes)} {
    for (auto i = 0u; i < dataTypes.size(); i++) {
        std::vector<LogicalType> chunkCollectionTypes;
        chunkCollectionTypes.push_back(*dataTypes[i]->copy());
        LocalDataChunkCollection localDataChunkCollection(mm, std::move(chunkCollectionTypes));
        updateChunks.push_back(std::move(localDataChunkCollection));
    }
}

bool LocalNodeGroup::hasUpdatesOrDeletions() const {
    if (!deleteInfo.isEmpty()) {
        return true;
    }
    for (auto& updateChunk : updateChunks) {
        if (!updateChunk.isEmpty()) {
            return true;
        }
    }
    return false;
}

void LocalDataChunkCollection::readValueAtRowIdx(
    row_idx_t rowIdx, column_id_t columnID, ValueVector* outputVector, sel_t posInOutputVector) {
    outputVector->copyFromVectorData(posInOutputVector,
        dataChunkCollection.getChunkUnsafe(rowIdx >> DEFAULT_VECTOR_CAPACITY_LOG_2)
            .getValueVector(columnID)
            .get(),
        rowIdx % DEFAULT_VECTOR_CAPACITY);
}

bool LocalDataChunkCollection::read(
    offset_t offset, column_id_t columnID, ValueVector* outputVector, sel_t posInOutputVector) {
    if (!offsetToRowIdx.contains(offset)) {
        return false;
    }
    auto rowIdx = offsetToRowIdx.at(offset);
    readValueAtRowIdx(rowIdx, columnID, outputVector, posInOutputVector);
    return true;
}

void LocalDataChunkCollection::update(
    offset_t offset, column_id_t columnID, ValueVector* propertyVector) {
    KU_ASSERT(offsetToRowIdx.contains(offset));
    auto rowIdx = offsetToRowIdx.at(offset);
    dataChunkCollection.getChunkUnsafe(rowIdx >> DEFAULT_VECTOR_CAPACITY_LOG_2)
        .getValueVector(columnID)
        ->copyFromVectorData(rowIdx % DEFAULT_VECTOR_CAPACITY, propertyVector,
            propertyVector->state->selVector->selectedPositions[0]);
}

void LocalDataChunkCollection::remove(offset_t srcNodeOffset, offset_t relOffset) {
    KU_ASSERT(srcNodeOffsetToRelOffsets.contains(srcNodeOffset));
    remove(relOffset);
    offsetToRowIdx.erase(relOffset);
    auto& vec = srcNodeOffsetToRelOffsets.at(srcNodeOffset);
    vec.erase(std::remove(vec.begin(), vec.end(), relOffset), vec.end());
    if (vec.empty()) {
        srcNodeOffsetToRelOffsets.erase(srcNodeOffset);
    }
}

row_idx_t LocalDataChunkCollection::appendToDataChunkCollection(std::vector<ValueVector*> vectors) {
    KU_ASSERT(vectors.size() == dataTypes.size());
    if (dataChunkCollection.getNumChunks() == 0 ||
        dataChunkCollection.getChunkUnsafe(dataChunkCollection.getNumChunks() - 1)
                .state->selVector->selectedSize == DEFAULT_VECTOR_CAPACITY) {
        dataChunkCollection.merge(createNewDataChunk());
    }
    auto& lastDataChunk =
        dataChunkCollection.getChunkUnsafe(dataChunkCollection.getNumChunks() - 1);
    for (auto i = 0u; i < vectors.size(); i++) {
        auto localVector = lastDataChunk.getValueVector(i);
        KU_ASSERT(vectors[i]->state->selVector->selectedSize == 1);
        auto pos = vectors[i]->state->selVector->selectedPositions[0];
        localVector->copyFromVectorData(
            lastDataChunk.state->selVector->selectedSize, vectors[i], pos);
    }
    lastDataChunk.state->selVector->selectedSize++;
    KU_ASSERT((dataChunkCollection.getNumChunks() - 1) * DEFAULT_VECTOR_CAPACITY +
                  lastDataChunk.state->selVector->selectedSize ==
              numRows + 1);
    return numRows++;
}

common::DataChunk LocalDataChunkCollection::createNewDataChunk() {
    DataChunk newDataChunk(dataTypes.size());
    for (auto i = 0u; i < dataTypes.size(); i++) {
        auto valueVector = std::make_unique<ValueVector>(dataTypes[i], mm);
        newDataChunk.insert(i, std::move(valueVector));
    }
    newDataChunk.state->selVector->resetSelectorToValuePosBuffer();
    return newDataChunk;
}

bool LocalTableData::insert(
    std::vector<ValueVector*> nodeIDVectors, std::vector<ValueVector*> propertyVectors) {
    KU_ASSERT(nodeIDVectors.size() >= 1);
    auto localNodeGroup = getOrCreateLocalNodeGroup(nodeIDVectors[0]);
    return localNodeGroup->insert(nodeIDVectors, propertyVectors);
}

bool LocalTableData::update(
    std::vector<ValueVector*> nodeIDVectors, column_id_t columnID, ValueVector* propertyVector) {
    KU_ASSERT(nodeIDVectors.size() >= 1);
    auto localNodeGroup = getOrCreateLocalNodeGroup(nodeIDVectors[0]);
    return localNodeGroup->update(nodeIDVectors, columnID, propertyVector);
}

bool LocalTableData::delete_(ValueVector* nodeIDVector, ValueVector* extraVector) {
    auto localNodeGroup = getOrCreateLocalNodeGroup(nodeIDVector);
    return localNodeGroup->delete_(nodeIDVector, extraVector);
}

LocalTableData* LocalTable::getOrCreateLocalTableData(
    const std::vector<std::unique_ptr<Column>>& columns, MemoryManager* mm, vector_idx_t dataIdx,
    RelMultiplicity multiplicity) {
    if (localTableDataCollection.empty()) {
        std::vector<LogicalType*> dataTypes;
        dataTypes.reserve(columns.size());
        for (auto& column : columns) {
            dataTypes.push_back(&column->getDataType());
        }
        switch (tableType) {
        case TableType::NODE: {
            localTableDataCollection.reserve(1);
            localTableDataCollection.push_back(
                std::make_unique<LocalNodeTableData>(std::move(dataTypes), mm));
        } break;
        case TableType::REL: {
            KU_ASSERT(dataIdx < 2);
            localTableDataCollection.resize(2);
            localTableDataCollection[dataIdx] =
                std::make_unique<LocalRelTableData>(multiplicity, std::move(dataTypes), mm);
        } break;
        default: {
            KU_UNREACHABLE;
        }
        }
    }
    KU_ASSERT(dataIdx < localTableDataCollection.size());
    if (!localTableDataCollection[dataIdx]) {
        KU_ASSERT(tableType == TableType::REL);
        std::vector<LogicalType*> dataTypes;
        dataTypes.reserve(columns.size());
        for (auto& column : columns) {
            dataTypes.push_back(&column->getDataType());
        }
        localTableDataCollection[dataIdx] =
            std::make_unique<LocalRelTableData>(multiplicity, std::move(dataTypes), mm);
    }
    KU_ASSERT(localTableDataCollection[dataIdx] != nullptr);
    return localTableDataCollection[dataIdx].get();
}

} // namespace storage
} // namespace kuzu
