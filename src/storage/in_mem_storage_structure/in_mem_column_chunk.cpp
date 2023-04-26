#include "storage/in_mem_storage_structure/in_mem_column_chunk.h"

#include <regex>

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
void InMemColumnChunk::templateCopyValuesToPage<std::string, common::CopyDescription&,
    common::offset_t>(const PageElementCursor& pageCursor, arrow::Array& array, uint64_t posInArray,
    uint64_t numValues, common::CopyDescription& copyDesc, common::offset_t nodeOffset) {
    copyStructValueToFields(array, posInArray, copyDesc, nodeOffset, numValues);
}

template<>
void InMemColumnChunk::setValueFromString<bool>(
    const char* value, uint64_t length, common::page_idx_t pageIdx, uint64_t posInPage) {
    std::istringstream boolStream{std::string(value)};
    bool booleanVal;
    boolStream >> std::boolalpha >> booleanVal;
    copyValue(pageIdx, posInPage, (uint8_t*)&booleanVal);
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

InMemStructColumnChunk::InMemStructColumnChunk(
    common::DataType dataType, common::offset_t startOffset, common::offset_t endOffset) {
    this->dataType = std::move(dataType);
    auto childrenTypes =
        reinterpret_cast<common::StructTypeInfo*>(this->dataType.getExtraTypeInfo())
            ->getChildrenTypes();
    for (auto& childType : childrenTypes) {
        inMemColumnChunksForFields.push_back(InMemColumnChunkFactory::getInMemColumnChunk(
            *childType, startOffset, endOffset, common::Types::getDataTypeSize(*childType),
            common::BufferPoolConstants::PAGE_4KB_SIZE /
                common::Types::getDataTypeSize(*childType)));
    }
}

void InMemStructColumnChunk::copyStructValueToFields(arrow::Array& array, uint64_t posInArray,
    const common::CopyDescription& copyDescription, common::offset_t startOffset,
    uint64_t numValues) {
    // TODO(Ziyi): support null values in struct.
    for (auto i = 0u; i < numValues; i++) {
        auto& stringArray = (arrow::StringArray&)array;
        auto structView = stringArray.GetView(i + posInArray);
        auto structTypeInfo =
            reinterpret_cast<common::StructTypeInfo*>(dataType.getExtraTypeInfo());
        auto structFieldTypes = structTypeInfo->getChildrenTypes();
        auto structFieldNames = structTypeInfo->getChildrenNames();
        std::regex whiteSpacePattern{"\\s"};
        // Removes the leading and trailing '{', '}';
        auto structString =
            std::string(structView.data(), structView.length()).substr(1, structView.length() - 2);
        auto structFields = common::StringUtils::split(structString, ",");
        for (auto& structField : structFields) {
            auto delimPos = structField.find(':');
            auto structFieldName =
                std::regex_replace(structField.substr(0, delimPos), whiteSpacePattern, "");
            auto structFieldIdx = getStructFieldIdx(structFieldNames, structFieldName);
            auto structFieldValue = structField.substr(delimPos + 1);
            copyValueToStructColumnField(i + startOffset, structFieldIdx, structFieldValue,
                *structFieldTypes[structFieldIdx]);
        }
    }
}

std::vector<InMemColumnChunk*> InMemStructColumnChunk::getInMemColumnChunksForFields() {
    std::vector<InMemColumnChunk*> columnChunksForFields;
    for (auto& inMemColumnChunksForField : inMemColumnChunksForFields) {
        columnChunksForFields.push_back(inMemColumnChunksForField.get());
    }
    return columnChunksForFields;
}

uint64_t InMemStructColumnChunk::getMinNumValuesLeftOnPage(common::offset_t nodeOffset) {
    auto minNumValuesLeftOnPage = UINT64_MAX;
    for (auto& inMemColumnChunkForField : inMemColumnChunksForFields) {
        auto fieldPageCursor = CursorUtils::getPageElementCursor(
            nodeOffset, inMemColumnChunkForField->getNumElementsInAPage());
        minNumValuesLeftOnPage = std::min(minNumValuesLeftOnPage,
            inMemColumnChunkForField->getNumElementsInAPage() - fieldPageCursor.elemPosInPage);
    }
    return minNumValuesLeftOnPage;
}

common::field_idx_t InMemStructColumnChunk::getStructFieldIdx(
    std::vector<std::string> structFieldNames, std::string structFieldName) {
    common::StringUtils::toUpper(structFieldName);
    auto it = std::find_if(structFieldNames.begin(), structFieldNames.end(),
        [structFieldName](const std::string& name) { return name == structFieldName; });
    if (it == structFieldNames.end()) {
        throw common::ConversionException{
            common::StringUtils::string_format("Invalid struct field name: {}.", structFieldName)};
    }
    return std::distance(structFieldNames.begin(), it);
}

void InMemStructColumnChunk::copyValueToStructColumnField(common::offset_t nodeOffset,
    common::field_idx_t structFieldIdx, const std::string& structFieldValue,
    const common::DataType& dataType) {
    auto inMemColumnChunkForField = inMemColumnChunksForFields[structFieldIdx].get();
    auto pageCursor = CursorUtils::getPageElementCursor(
        nodeOffset, inMemColumnChunkForField->getNumElementsInAPage());
    switch (dataType.typeID) {
    case common::INT64: {
        inMemColumnChunkForField->setValueFromString<int64_t>(structFieldValue.c_str(),
            structFieldValue.length(), pageCursor.pageIdx, pageCursor.elemPosInPage);
    } break;
    case common::INT32: {
        inMemColumnChunkForField->setValueFromString<int32_t>(structFieldValue.c_str(),
            structFieldValue.length(), pageCursor.pageIdx, pageCursor.elemPosInPage);
    } break;
    case common::INT16: {
        inMemColumnChunkForField->setValueFromString<int16_t>(structFieldValue.c_str(),
            structFieldValue.length(), pageCursor.pageIdx, pageCursor.elemPosInPage);
    } break;
    case common::DOUBLE: {
        inMemColumnChunkForField->setValueFromString<double_t>(structFieldValue.c_str(),
            structFieldValue.length(), pageCursor.pageIdx, pageCursor.elemPosInPage);
    } break;
    case common::FLOAT: {
        inMemColumnChunkForField->setValueFromString<float_t>(structFieldValue.c_str(),
            structFieldValue.length(), pageCursor.pageIdx, pageCursor.elemPosInPage);
    } break;
    case common::BOOL: {
        inMemColumnChunkForField->setValueFromString<bool>(structFieldValue.c_str(),
            structFieldValue.length(), pageCursor.pageIdx, pageCursor.elemPosInPage);
    } break;
    case common::DATE: {
        inMemColumnChunkForField->setValueFromString<common::date_t>(structFieldValue.c_str(),
            structFieldValue.length(), pageCursor.pageIdx, pageCursor.elemPosInPage);
    } break;
    case common::TIMESTAMP: {
        inMemColumnChunkForField->setValueFromString<common::timestamp_t>(structFieldValue.c_str(),
            structFieldValue.length(), pageCursor.pageIdx, pageCursor.elemPosInPage);
    } break;
    case common::INTERVAL: {
        inMemColumnChunkForField->setValueFromString<common::interval_t>(structFieldValue.c_str(),
            structFieldValue.length(), pageCursor.pageIdx, pageCursor.elemPosInPage);
    } break;
    default:
        assert(false);
    }
}

std::unique_ptr<InMemColumnChunk> InMemColumnChunkFactory::getInMemColumnChunk(
    common::DataType dataType, common::offset_t startOffset, common::offset_t endOffset,
    uint16_t numBytesForElement, uint64_t numElementsInAPage) {
    if (dataType.typeID == common::STRUCT) {
        return std::make_unique<InMemStructColumnChunk>(
            std::move(dataType), startOffset, endOffset);
    } else {
        return std::make_unique<InMemColumnChunk>(
            std::move(dataType), startOffset, endOffset, numBytesForElement, numElementsInAPage);
    }
}

} // namespace storage
} // namespace kuzu
