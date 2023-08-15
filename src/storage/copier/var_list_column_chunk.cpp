#include "storage/copier/var_list_column_chunk.h"

#include "arrow/array.h"
#include "storage/copier/table_copy_utils.h"

namespace kuzu {
namespace storage {

void VarListDataColumnChunk::reset() {
    dataColumnChunk->resetToEmpty();
}

void VarListDataColumnChunk::resizeBuffer(uint64_t numValues) {
    if (numValues <= capacity) {
        return;
    }
    while (capacity < numValues) {
        capacity *= VAR_LIST_RESIZE_RATIO;
    }
    dataColumnChunk->resize(capacity);
}

VarListColumnChunk::VarListColumnChunk(LogicalType dataType, CopyDescription* copyDescription)
    : ColumnChunk{std::move(dataType), copyDescription, true /* hasNullChunk */},
      varListDataColumnChunk{ColumnChunkFactory::createColumnChunk(
          *VarListType::getChildType(&this->dataType), copyDescription)} {
    assert(this->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
}

void VarListColumnChunk::append(
    arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    assert(array->type_id() == arrow::Type::STRING || array->type_id() == arrow::Type::LIST ||
           array->type_id() == arrow::Type::LARGE_LIST);
    switch (array->type_id()) {
    case arrow::Type::STRING: {
        copyVarListFromArrowString(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::LIST: {
        copyVarListFromArrowList<arrow::ListArray>(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::LARGE_LIST: {
        copyVarListFromArrowList<arrow::LargeListArray>(array, startPosInChunk, numValuesToAppend);
    } break;
    default: {
        throw NotImplementedException("ListColumnChunk::appendArray");
    }
    }
    numValues += numValuesToAppend;
}

void VarListColumnChunk::append(ColumnChunk* other, offset_t startPosInOtherChunk,
    offset_t startPosInChunk, uint32_t numValuesToAppend) {
    nullChunk->append(
        other->getNullChunk(), startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    auto otherListChunk = reinterpret_cast<VarListColumnChunk*>(other);
    auto offsetInDataChunkToAppend = varListDataColumnChunk.getNumValues();
    for (auto i = 0u; i < numValuesToAppend; i++) {
        offsetInDataChunkToAppend += otherListChunk->getListLen(startPosInOtherChunk + i);
        setValue(offsetInDataChunkToAppend, startPosInChunk + i);
    }
    auto startOffset = otherListChunk->getListOffset(startPosInOtherChunk);
    auto endOffset = otherListChunk->getListOffset(startPosInOtherChunk + numValuesToAppend);
    varListDataColumnChunk.resizeBuffer(offsetInDataChunkToAppend);
    varListDataColumnChunk.dataColumnChunk->append(
        otherListChunk->varListDataColumnChunk.dataColumnChunk.get(), startOffset,
        varListDataColumnChunk.getNumValues(), endOffset - startOffset);
    numValues += numValuesToAppend;
}

void VarListColumnChunk::copyVarListFromArrowString(
    arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    auto stringArray = (arrow::StringArray*)array;
    auto arrayData = stringArray->data();
    if (arrayData->MayHaveNulls()) {
        for (auto i = 0u; i < numValuesToAppend; i++) {
            auto posInChunk = startPosInChunk + i;
            if (arrayData->IsNull(i)) {
                nullChunk->setNull(posInChunk, true);
                setValue(varListDataColumnChunk.getNumValues(), posInChunk);
                continue;
            }
            auto value = stringArray->GetView(i);
            auto listVal = TableCopyUtils::getVarListValue(
                value.data(), 1, value.size() - 2, dataType, *copyDescription);
            write(*listVal, posInChunk);
        }
    } else {
        for (auto i = 0u; i < numValuesToAppend; i++) {
            auto value = stringArray->GetView(i);
            auto posInChunk = startPosInChunk + i;
            auto listVal = TableCopyUtils::getVarListValue(
                value.data(), 1, value.size() - 2, dataType, *copyDescription);
            write(*listVal, posInChunk);
        }
    }
}

void VarListColumnChunk::write(const Value& listVal, uint64_t posToWrite) {
    assert(listVal.getDataType()->getPhysicalType() == PhysicalTypeID::VAR_LIST);
    auto numValuesInList = NestedVal::getChildrenSize(&listVal);
    varListDataColumnChunk.resizeBuffer(varListDataColumnChunk.getNumValues() + numValuesInList);
    for (auto i = 0u; i < numValuesInList; i++) {
        varListDataColumnChunk.dataColumnChunk->write(
            *NestedVal::getChildVal(&listVal, i), varListDataColumnChunk.getNumValues());
        varListDataColumnChunk.increaseNumValues(1);
    }
    setValue(varListDataColumnChunk.getNumValues(), posToWrite);
}

void VarListColumnChunk::setValueFromString(const char* value, uint64_t length, uint64_t pos) {
    auto listVal =
        TableCopyUtils::getVarListValue(value, 1, length - 2, dataType, *copyDescription);
    write(*listVal, pos);
}

void VarListColumnChunk::resetToEmpty() {
    ColumnChunk::resetToEmpty();
    varListDataColumnChunk.reset();
}

void VarListColumnChunk::append(common::ValueVector* vector, common::offset_t startPosInChunk) {
    auto nextListOffsetInChunk = getListOffset(startPosInChunk);
    auto offsetBufferToWrite = (common::offset_t*)(buffer.get());
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        auto pos = vector->state->selVector->selectedPositions[i];
        uint64_t listLen = vector->isNull(pos) ? 0 : vector->getValue<list_entry_t>(pos).size;
        nullChunk->setNull(startPosInChunk + i, vector->isNull(pos));
        nextListOffsetInChunk += listLen;
        offsetBufferToWrite[startPosInChunk + i] = nextListOffsetInChunk;
    }
    varListDataColumnChunk.resizeBuffer(nextListOffsetInChunk);
    auto dataVector = ListVector::getDataVector(vector);
    dataVector->state = std::make_unique<DataChunkState>();
    dataVector->state->selVector->resetSelectorToValuePosBuffer();
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        auto listEntry =
            vector->getValue<list_entry_t>(vector->state->selVector->selectedPositions[i]);
        dataVector->state->selVector->selectedSize = listEntry.size;
        for (auto j = 0u; j < listEntry.size; j++) {
            dataVector->state->selVector->selectedPositions[j] = listEntry.offset + j;
        }
        varListDataColumnChunk.append(dataVector);
    }
    numValues += vector->state->selVector->selectedSize;
}

} // namespace storage
} // namespace kuzu
