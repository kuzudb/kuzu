#include "storage/local_storage/local_table.h"

#include "storage/local_storage/local_node_table.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/store/column.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void LocalVector::read(
    sel_t offsetInLocalVector, ValueVector* resultVector, sel_t offsetInResultVector) {
    resultVector->copyFromVectorData(offsetInResultVector, vector.get(), offsetInLocalVector);
}

void LocalVector::append(ValueVector* valueVector) {
    KU_ASSERT(valueVector->state->selVector->selectedSize == 1);
    auto pos = valueVector->state->selVector->selectedPositions[0];
    vector->copyFromVectorData(numValues, valueVector, pos);
    numValues++;
}

void LocalVectorCollection::read(
    row_idx_t rowIdx, ValueVector* outputVector, sel_t posInOutputVector) {
    auto vectorIdx = rowIdx >> DEFAULT_VECTOR_CAPACITY_LOG_2;
    auto offsetInVector = rowIdx & (DEFAULT_VECTOR_CAPACITY - 1);
    KU_ASSERT(vectorIdx < vectors.size());
    vectors[vectorIdx]->read(offsetInVector, outputVector, posInOutputVector);
}

row_idx_t LocalVectorCollection::append(ValueVector* vector) {
    prepareAppend();
    auto lastVector = vectors.back().get();
    KU_ASSERT(!lastVector->isFull());
    lastVector->append(vector);
    return numRows++;
}

void LocalVectorCollection::prepareAppend() {
    if (vectors.empty()) {
        vectors.emplace_back(std::make_unique<LocalVector>(*dataType, mm));
    }
    auto lastVector = vectors.back().get();
    if (lastVector->isFull()) {
        vectors.emplace_back(std::make_unique<LocalVector>(*dataType, mm));
    }
}

std::unique_ptr<LocalVectorCollection> LocalVectorCollection::getStructChildVectorCollection(
    common::struct_field_idx_t idx) {
    auto childCollection = std::make_unique<LocalVectorCollection>(
        StructType::getField(dataType.get(), idx)->getType()->copy(), mm);

    for (int i = 0; i < numRows; i++) {
        auto fieldVector =
            common::StructVector::getFieldVector(getLocalVector(i)->getVector(), idx);
        childCollection->append(fieldVector.get());
    }
    return childCollection;
}

LocalNodeGroup::LocalNodeGroup(
    offset_t nodeGroupStartOffset, std::vector<LogicalType*> dataTypes, MemoryManager* mm)
    : nodeGroupStartOffset{nodeGroupStartOffset} {
    chunks.resize(dataTypes.size());
    for (auto i = 0u; i < dataTypes.size(); ++i) {
        // To avoid unnecessary memory consumption, we chunk local changes of each column in the
        // node group into chunks of size DEFAULT_VECTOR_CAPACITY.
        chunks[i] = std::make_unique<LocalVectorCollection>(dataTypes[i]->copy(), mm);
    }
}

LocalTableData* LocalTable::getOrCreateLocalTableData(
    const std::vector<std::unique_ptr<Column>>& columns, MemoryManager* mm,
    ColumnDataFormat dataFormat, vector_idx_t dataIdx) {
    if (localTableDataCollection.empty()) {
        std::vector<LogicalType*> dataTypes;
        for (auto& column : columns) {
            dataTypes.push_back(column->getDataType());
        }
        switch (tableType) {
        case TableType::NODE: {
            KU_ASSERT(dataFormat == ColumnDataFormat::REGULAR);
            localTableDataCollection.reserve(1);
            localTableDataCollection.push_back(
                std::make_unique<LocalNodeTableData>(std::move(dataTypes), mm, dataFormat));
        } break;
        case TableType::REL: {
            KU_ASSERT(dataIdx < 2);
            localTableDataCollection.resize(2);
            localTableDataCollection[dataIdx] =
                std::make_unique<LocalRelTableData>(std::move(dataTypes), mm, dataFormat);
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
        for (auto& column : columns) {
            dataTypes.push_back(column->getDataType());
        }
        localTableDataCollection[dataIdx] =
            std::make_unique<LocalRelTableData>(std::move(dataTypes), mm, dataFormat);
    }
    KU_ASSERT(localTableDataCollection[dataIdx] != nullptr);
    return localTableDataCollection[dataIdx].get();
}

} // namespace storage
} // namespace kuzu
