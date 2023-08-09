#include "storage/copier/var_list_column_chunk.h"

#include "arrow/array.h"
#include "storage/copier/table_copy_utils.h"

namespace kuzu {
namespace storage {

void VarListDataColumnChunk::reset() {
    dataChunk->resetToEmpty();
    numValuesInDataChunk = 0;
}

void VarListDataColumnChunk::resize(uint64_t numValues) {
    if (numValues <= capacityInDataChunk) {
        return;
    }
    while (capacityInDataChunk < numValues) {
        capacityInDataChunk *= VAR_LIST_RESIZE_RATIO;
    }
    dataChunk->resize(capacityInDataChunk);
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
}

void VarListColumnChunk::append(ColumnChunk* other, offset_t startPosInOtherChunk,
    offset_t startPosInChunk, uint32_t numValuesToAppend) {
    nullChunk->append(
        other->getNullChunk(), startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    auto otherListChunk = reinterpret_cast<VarListColumnChunk*>(other);
    auto offsetInDataChunkToAppend = varListDataColumnChunk.numValuesInDataChunk;
    for (auto i = 0u; i < numValuesToAppend; i++) {
        varListDataColumnChunk.numValuesInDataChunk +=
            otherListChunk->getListLen(startPosInOtherChunk + i);
        setValue(varListDataColumnChunk.numValuesInDataChunk, startPosInChunk + i);
    }
    auto startOffset = otherListChunk->getListOffset(startPosInOtherChunk);
    auto endOffset = otherListChunk->getListOffset(startPosInOtherChunk + numValuesToAppend);
    varListDataColumnChunk.resize(varListDataColumnChunk.numValuesInDataChunk);
    varListDataColumnChunk.dataChunk->append(otherListChunk->varListDataColumnChunk.dataChunk.get(),
        startOffset, offsetInDataChunkToAppend, endOffset - startOffset);
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
                setValue(varListDataColumnChunk.numValuesInDataChunk, posInChunk);
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
    varListDataColumnChunk.resize(varListDataColumnChunk.numValuesInDataChunk + numValuesInList);
    for (auto i = 0u; i < numValuesInList; i++) {
        varListDataColumnChunk.dataChunk->write(
            *NestedVal::getChildVal(&listVal, i), varListDataColumnChunk.numValuesInDataChunk);
        varListDataColumnChunk.numValuesInDataChunk++;
    }
    setValue(varListDataColumnChunk.numValuesInDataChunk, posToWrite);
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

} // namespace storage
} // namespace kuzu
