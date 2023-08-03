#include "storage/copier/var_list_column_chunk.h"

#include "arrow/array.h"
#include "storage/copier/table_copy_utils.h"

namespace kuzu {
namespace storage {

VarListColumnChunk::VarListColumnChunk(LogicalType dataType, CopyDescription* copyDescription)
    : ColumnChunk{std::move(dataType), copyDescription, true /* hasNullChunk */},
      varListDataColumnChunk{ColumnChunkFactory::createColumnChunk(
          *VarListType::getChildType(&this->dataType), copyDescription)} {
    assert(this->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
}

void VarListColumnChunk::append(
    arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    assert(array->type_id() == arrow::Type::STRING || array->type_id() == arrow::Type::LIST);
    switch (array->type_id()) {
    case arrow::Type::STRING: {
        copyVarListFromArrowString(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::LIST: {
        copyVarListFromArrowList(array, startPosInChunk, numValuesToAppend);
    } break;
    default: {
        throw NotImplementedException("ListColumnChunk::appendArray");
    }
    }
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
            auto listVal = TableCopyUtils::getArrowVarList(
                value.data(), 1, value.size() - 2, dataType, *copyDescription);
            write(*listVal, posInChunk);
        }
    } else {
        for (auto i = 0u; i < numValuesToAppend; i++) {
            auto value = stringArray->GetView(i);
            auto posInChunk = startPosInChunk + i;
            auto listVal = TableCopyUtils::getArrowVarList(
                value.data(), 1, value.size() - 2, dataType, *copyDescription);
            write(*listVal, posInChunk);
        }
    }
}

void VarListColumnChunk::copyVarListFromArrowList(
    arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    //    auto listArray = (arrow::ListArray*)array;
    //    auto offsetArray = listArray->offsets()->Slice(1 /* offset */);
    //    append(offsetArray.get(), startPosInChunk, numValuesToAppend);
    //    if (offsetArray->data()->MayHaveNulls()) {
    //        for (auto i = 0u; i < numValuesToAppend; i++) {
    //            nullChunk->setNull(i + startPosInChunk, offsetArray->data()->IsNull(i));
    //        }
    //    } else {
    //        nullChunk->setRangeNoNull(startPosInChunk, numValuesToAppend);
    //    }
    //    auto startOffset = listArray->value_offset(startPosInChunk);
    //    auto endOffset = listArray->value_offset(startPosInChunk + numValuesToAppend);
    //    varListDataColumnChunk.dataChunk->resize(endOffset - startOffset);
    //    varListDataColumnChunk.dataChunk->append(
    //        listArray->offsets().get(), startOffset, endOffset - startOffset);
}

void VarListColumnChunk::write(const common::Value& listVal, uint64_t posToWrite) {
    assert(listVal.getDataType()->getPhysicalType() == PhysicalTypeID::VAR_LIST);
    auto numValuesInList = NestedVal::getChildrenSize(&listVal);
    resizeDataChunk(varListDataColumnChunk.numValuesInDataChunk + numValuesInList);
    for (auto i = 0u; i < numValuesInList; i++) {
        varListDataColumnChunk.dataChunk->write(
            *NestedVal::getChildVal(&listVal, i), varListDataColumnChunk.numValuesInDataChunk);
        varListDataColumnChunk.numValuesInDataChunk++;
    }
    setValue(varListDataColumnChunk.numValuesInDataChunk, posToWrite);
}

void VarListColumnChunk::setValueFromString(const char* value, uint64_t length, uint64_t pos) {
    auto listVal =
        TableCopyUtils::getArrowVarList(value, 1, length - 2, dataType, *copyDescription);
    write(*listVal, pos);
}

void VarListColumnChunk::resizeDataChunk(uint64_t numValues) {
    if (numValues <= varListDataColumnChunk.capacityInDataChunk) {
        return;
    }
    while (varListDataColumnChunk.capacityInDataChunk < numValues) {
        varListDataColumnChunk.capacityInDataChunk *= 2;
    }
    varListDataColumnChunk.dataChunk->resize(varListDataColumnChunk.capacityInDataChunk);
}

} // namespace storage
} // namespace kuzu
