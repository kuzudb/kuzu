#include "storage/store/var_sized_column_chunk.h"

#include "storage/copier/table_copy_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

VarSizedColumnChunk::VarSizedColumnChunk(LogicalType dataType, CopyDescription* copyDescription)
    : ColumnChunk{std::move(dataType), copyDescription} {
    overflowFile = std::make_unique<InMemOverflowFile>();
}

void VarSizedColumnChunk::resetToEmpty() {
    ColumnChunk::resetToEmpty();
    overflowFile = std::make_unique<InMemOverflowFile>();
    overflowCursor.resetValue();
}

void VarSizedColumnChunk::appendArray(
    arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    assert(array->type_id() == arrow::Type::STRING || array->type_id() == arrow::Type::LIST);
    switch (array->type_id()) {
    case arrow::Type::STRING: {
        switch (dataType.getLogicalTypeID()) {
        case LogicalTypeID::STRING: {
            templateCopyVarSizedValuesFromString<ku_string_t>(
                array, startPosInChunk, numValuesToAppend);
        } break;
        case LogicalTypeID::VAR_LIST: {
            templateCopyVarSizedValuesFromString<ku_list_t>(
                array, startPosInChunk, numValuesToAppend);
        } break;
        default: {
            throw NotImplementedException(
                "Unsupported VarSizedColumnChunk::appendArray for string array");
        }
        }
    } break;
    case arrow::Type::LIST: {
        copyValuesFromVarList(array, startPosInChunk, numValuesToAppend);
    } break;
    default: {
        throw NotImplementedException("VarSizedColumnChunk::appendArray");
    }
    }
}

void VarSizedColumnChunk::appendColumnChunk(ColumnChunk* other, offset_t startPosInOtherChunk,
    offset_t startPosInChunk, uint32_t numValuesToAppend) {
    auto otherChunk = dynamic_cast<VarSizedColumnChunk*>(other);
    nullChunk->appendColumnChunk(
        otherChunk->getNullChunk(), startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::STRING: {
        appendStringColumnChunk(
            otherChunk, startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    } break;
    case LogicalTypeID::VAR_LIST: {
        appendVarListColumnChunk(
            otherChunk, startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    } break;
    default: {
        throw NotImplementedException("VarSizedColumnChunk::appendColumnChunk");
    }
    }
}

page_idx_t VarSizedColumnChunk::flushBuffer(
    BMFileHandle* nodeGroupsDataFH, page_idx_t startPageIdx) {
    ColumnChunk::flushBuffer(nodeGroupsDataFH, startPageIdx);
    startPageIdx += ColumnChunk::getNumPagesForBuffer();
    for (auto i = 0u; i < overflowFile->getNumPages(); i++) {
        FileUtils::writeToFile(nodeGroupsDataFH->getFileInfo(), overflowFile->getPage(i)->data,
            BufferPoolConstants::PAGE_4KB_SIZE, startPageIdx * BufferPoolConstants::PAGE_4KB_SIZE);
        startPageIdx++;
    }
    return getNumPagesForBuffer();
}

void VarSizedColumnChunk::appendStringColumnChunk(VarSizedColumnChunk* other,
    offset_t startPosInOtherChunk, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    PageByteCursor cursorToCopyFrom;
    auto otherKuVals = (ku_string_t*)(other->buffer.get());
    auto kuVals = (ku_string_t*)(buffer.get());
    for (auto i = 0u; i < numValuesToAppend; i++) {
        kuVals[i + startPosInChunk] = otherKuVals[i + startPosInOtherChunk];
        if (kuVals[i + startPosInChunk].len <= ku_string_t::SHORT_STR_LENGTH) {
            continue;
        }
        TypeUtils::decodeOverflowPtr(otherKuVals[i + startPosInOtherChunk].overflowPtr,
            cursorToCopyFrom.pageIdx, cursorToCopyFrom.offsetInPage);
        overflowFile->copyStringOverflow(overflowCursor,
            other->overflowFile->getPage(cursorToCopyFrom.pageIdx)->data +
                cursorToCopyFrom.offsetInPage,
            &kuVals[i + startPosInChunk]);
    }
}

void VarSizedColumnChunk::appendVarListColumnChunk(VarSizedColumnChunk* other,
    offset_t startPosInOtherChunk, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    PageByteCursor cursorToCopyFrom;
    auto otherKuVals = (ku_list_t*)(other->buffer.get());
    auto kuVals = (ku_list_t*)(buffer.get());
    for (auto i = 0u; i < numValuesToAppend; i++) {
        auto kuListToCopyFrom = otherKuVals[i + startPosInOtherChunk];
        auto kuListToCopyInto = kuVals[i + startPosInChunk];
        TypeUtils::decodeOverflowPtr(
            kuListToCopyFrom.overflowPtr, cursorToCopyFrom.pageIdx, cursorToCopyFrom.offsetInPage);
        overflowFile->copyListOverflowFromFile(other->overflowFile.get(), cursorToCopyFrom,
            overflowCursor, &kuListToCopyInto, VarListType::getChildType(&dataType));
    }
}

template<typename T>
void VarSizedColumnChunk::templateCopyVarSizedValuesFromString(
    arrow::Array* array, common::offset_t startPosInChunk, uint32_t numValuesToAppend) {
    auto stringArray = (arrow::StringArray*)array;
    auto arrayData = stringArray->data();
    if (arrayData->MayHaveNulls()) {
        for (auto i = 0u; i < numValuesToAppend; i++) {
            auto posInChunk = startPosInChunk + i;
            if (arrayData->IsNull(i)) {
                nullChunk->setNull(posInChunk, true);
                continue;
            }
            auto value = stringArray->GetView(i);
            setValueFromString<T>(value.data(), value.length(), posInChunk);
        }
    } else {
        for (auto i = 0u; i < numValuesToAppend; i++) {
            auto posInChunk = startPosInChunk + i;
            auto value = stringArray->GetView(i);
            setValueFromString<T>(value.data(), value.length(), posInChunk);
        }
    }
}

void VarSizedColumnChunk::copyValuesFromVarList(
    arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    assert(array->type_id() == arrow::Type::LIST);
    auto listArray = (arrow::ListArray*)array;
    auto listArrayData = listArray->data();
    if (listArrayData->MayHaveNulls()) {
        for (auto i = 0u; i < numValuesToAppend; i++) {
            auto posInChunk = startPosInChunk + i;
            if (listArrayData->IsNull(i)) {
                nullChunk->setNull(posInChunk, true);
                continue;
            }
            auto kuList = overflowFile->appendList(dataType, *listArray, i, overflowCursor);
            setValue(kuList, posInChunk);
        }
    } else {
        for (auto i = 0u; i < numValuesToAppend; i++) {
            auto posInChunk = startPosInChunk + i;
            auto kuList = overflowFile->appendList(dataType, *listArray, i, overflowCursor);
            setValue(kuList, posInChunk);
        }
    }
}

// STRING
template<>
void VarSizedColumnChunk::setValueFromString<ku_string_t>(
    const char* value, uint64_t length, uint64_t pos) {
    if (length > BufferPoolConstants::PAGE_4KB_SIZE) {
        length = BufferPoolConstants::PAGE_4KB_SIZE;
    }
    auto val = overflowFile->copyString(value, length, overflowCursor);
    setValue(val, pos);
}

// VAR_LIST
template<>
void VarSizedColumnChunk::setValueFromString<ku_list_t>(
    const char* value, uint64_t length, uint64_t pos) {
    auto varListVal =
        TableCopyUtils::getArrowVarList(value, 1, length - 2, dataType, *copyDescription);
    auto val = overflowFile->copyList(*varListVal, overflowCursor);
    setValue(val, pos);
}

// STRING
template<>
std::string VarSizedColumnChunk::getValue<std::string>(offset_t pos) const {
    auto kuStr = ((ku_string_t*)buffer.get())[pos];
    return overflowFile->readString(&kuStr);
}

} // namespace storage
} // namespace kuzu
