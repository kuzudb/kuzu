#include "storage/in_mem_storage_structure/in_mem_column_chunk.h"

#include "common/types/types.h"

namespace kuzu {
namespace storage {

InMemColumnChunk::InMemColumnChunk(common::DataType dataType, common::offset_t startOffset,
    common::offset_t endOffset, uint16_t numBytesForElement, uint64_t numElementsInAPage)
    : dataType{std::move(dataType)}, numBytesForElement{numBytesForElement},
      numElementsInAPage{numElementsInAPage} {
    startPageIdx = CursorUtils::getPageIdx(startOffset, numElementsInAPage);
    endPageIdx = CursorUtils::getPageIdx(endOffset, numElementsInAPage);
    auto numPages = endPageIdx - startPageIdx + 1;
    pages = std::make_unique<uint8_t[]>(numPages * common::BufferPoolConstants::PAGE_4KB_SIZE);
    memset(pages.get(), 0, numPages * common::BufferPoolConstants::PAGE_4KB_SIZE);
}

template<>
void InMemColumnChunk::templateCopyValuesToPage<bool>(const PageElementCursor& pageCursor,
    arrow::Array& array, uint64_t posInArray, uint64_t numValues) {
    auto& boolArray = (arrow::BooleanArray&)array;
    auto data = boolArray.data();
    auto valuesInPage = (bool*)(getPage(pageCursor.pageIdx));
    if (data->MayHaveNulls()) {
        for (auto i = 0u; i < numValues; i++) {
            if (data->IsNull(i + posInArray)) {
                continue;
            }
            valuesInPage[i + pageCursor.elemPosInPage] = boolArray.Value(i + posInArray);
        }
    } else {
        for (auto i = 0u; i < numValues; i++) {
            valuesInPage[i + pageCursor.elemPosInPage] = boolArray.Value(i + posInArray);
        }
    }
}

template<>
void InMemColumnChunk::templateCopyValuesToPage<std::string, InMemOverflowFile*, PageByteCursor&,
    common::CopyDescription&>(const PageElementCursor& pageCursor, arrow::Array& array,
    uint64_t posInArray, uint64_t numValues, InMemOverflowFile* overflowFile,
    PageByteCursor& overflowCursor, common::CopyDescription& copyDesc) {
    switch (dataType.typeID) {
    case common::DATE: {
        templateCopyValuesAsStringToPage<common::date_t>(pageCursor, array, posInArray, numValues);
    } break;
    case common::TIMESTAMP: {
        templateCopyValuesAsStringToPage<common::timestamp_t>(
            pageCursor, array, posInArray, numValues);
    } break;
    case common::INTERVAL: {
        templateCopyValuesAsStringToPage<common::interval_t>(
            pageCursor, array, posInArray, numValues);
    } break;
    case common::FIXED_LIST: {
        // Fixed list is a fixed-sized blob.
        templateCopyValuesAsStringToPage<uint8_t*, common::CopyDescription&>(
            pageCursor, array, posInArray, numValues, copyDesc);
    } break;
    case common::VAR_LIST: {
        templateCopyValuesAsStringToPage<common::ku_list_t, InMemOverflowFile*, PageByteCursor&,
            common::CopyDescription&>(
            pageCursor, array, posInArray, numValues, overflowFile, overflowCursor, copyDesc);
    } break;
    case common::STRING: {
        templateCopyValuesAsStringToPage<common::ku_string_t, InMemOverflowFile*, PageByteCursor&>(
            pageCursor, array, posInArray, numValues, overflowFile, overflowCursor);
    } break;
    default: {
        throw common::CopyException("Unsupported data type for string copy.");
    }
    }
}

template<>
void InMemColumnChunk::setValueFromString<common::ku_string_t, InMemOverflowFile*, PageByteCursor&>(
    const char* value, uint64_t length, common::page_idx_t pageIdx, uint64_t posInPage,
    InMemOverflowFile* overflowFile, PageByteCursor& overflowCursor) {
    if (length > common::BufferPoolConstants::PAGE_4KB_SIZE) {
        length = common::BufferPoolConstants::PAGE_4KB_SIZE;
    }
    auto val = overflowFile->copyString(value, length, overflowCursor);
    copyValue(pageIdx, posInPage, (uint8_t*)&val);
}

// Fixed list
template<>
void InMemColumnChunk::setValueFromString<uint8_t*, common::CopyDescription&>(const char* value,
    uint64_t length, common::page_idx_t pageIdx, uint64_t posInPage,
    common::CopyDescription& copyDescription) {
    auto fixedListVal =
        TableCopyExecutor::getArrowFixedList(value, 1, length - 2, dataType, copyDescription);
    copyValue(pageIdx, posInPage, fixedListVal.get());
}

// Var list
template<>
void InMemColumnChunk::setValueFromString<common::ku_list_t, InMemOverflowFile*, PageByteCursor&,
    common::CopyDescription&>(const char* value, uint64_t length, common::page_idx_t pageIdx,
    uint64_t posInPage, InMemOverflowFile* overflowFile, PageByteCursor& overflowCursor,
    common::CopyDescription& copyDescription) {
    auto varListVal =
        TableCopyExecutor::getArrowVarList(value, 1, length - 2, dataType, copyDescription);
    auto val = overflowFile->copyList(*varListVal, overflowCursor);
    copyValue(pageIdx, posInPage, (uint8_t*)&val);
}

template<>
void InMemColumnChunk::setValueFromString<common::interval_t>(
    const char* value, uint64_t length, common::page_idx_t pageIdx, uint64_t posInPage) {
    auto val = common::Interval::FromCString(value, length);
    copyValue(pageIdx, posInPage, (uint8_t*)&val);
}

template<>
void InMemColumnChunk::setValueFromString<common::date_t>(
    const char* value, uint64_t length, common::page_idx_t pageIdx, uint64_t posInPage) {
    auto val = common::Date::FromCString(value, length);
    copyValue(pageIdx, posInPage, (uint8_t*)&val);
}

template<>
void InMemColumnChunk::setValueFromString<common::timestamp_t>(
    const char* value, uint64_t length, common::page_idx_t pageIdx, uint64_t posInPage) {
    auto val = common::Timestamp::FromCString(value, length);
    copyValue(pageIdx, posInPage, (uint8_t*)&val);
}

} // namespace storage
} // namespace kuzu
