#include "storage/in_mem_storage_structure/in_mem_column_chunk.h"

#include <regex>

#include "common/types/types.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

InMemColumnChunk::InMemColumnChunk(
    LogicalType dataType, offset_t startNodeOffset, offset_t endNodeOffset, bool requireNullBits)
    : dataType{std::move(dataType)}, startNodeOffset{startNodeOffset} {
    numBytesPerValue = getDataTypeSizeInColumn(this->dataType);
    numBytes = numBytesPerValue * (endNodeOffset - startNodeOffset + 1);
    buffer = std::make_unique<uint8_t[]>(numBytes);
    if (requireNullBits) {
        nullChunk = std::make_unique<InMemColumnChunk>(
            LogicalType{LogicalTypeID::BOOL}, startNodeOffset, endNodeOffset, false /* hasNull */);
        memset(nullChunk->getData(), UINT8_MAX, nullChunk->getNumBytes());
    }
    // TODO(Guodong): Consider shifting to a hierarchy structure for STRING/LIST/STRUCT.
    if (this->dataType.getLogicalTypeID() == LogicalTypeID::STRUCT) {
        auto childTypes = common::StructType::getStructFieldTypes(&this->dataType);
        childChunks.resize(childTypes.size());
        for (auto i = 0u; i < childTypes.size(); i++) {
            childChunks[i] =
                std::make_unique<InMemColumnChunk>(*childTypes[i], startNodeOffset, endNodeOffset);
        }
    }
}

void InMemColumnChunk::setValue(uint8_t* val, offset_t pos) {
    memcpy(buffer.get() + (pos * numBytesPerValue), val, numBytesPerValue);
    nullChunk->setValue(false, pos);
}

void InMemColumnChunk::flush(FileInfo* walFileInfo) {
    if (numBytes > 0) {
        auto startFileOffset = startNodeOffset * numBytesPerValue;
        FileUtils::writeToFile(walFileInfo, buffer.get(), numBytes, startFileOffset);
    }
}

uint32_t InMemColumnChunk::getDataTypeSizeInColumn(common::LogicalType& dataType) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::STRUCT: {
        return 0;
    }
    case LogicalTypeID::INTERNAL_ID: {
        return sizeof(offset_t);
    }
    default: {
        return storage::StorageUtils::getDataTypeSize(dataType);
    }
    }
}

template<>
void InMemColumnChunk::templateCopyValuesToPage<bool>(arrow::Array& array) {
    auto& boolArray = (arrow::BooleanArray&)array;
    auto data = boolArray.data();
    auto valuesInChunk = (bool*)(buffer.get());
    if (data->MayHaveNulls()) {
        for (auto i = 0u; i < boolArray.length(); i++) {
            if (data->IsNull(i)) {
                continue;
            }
            valuesInChunk[i] = boolArray.Value(i);
            nullChunk->setValue<bool>(false, i);
        }
    } else {
        for (auto i = 0u; i < boolArray.length(); i++) {
            valuesInChunk[i] = boolArray.Value(i);
            nullChunk->setValue<bool>(false, i);
        }
    }
}

template<>
void InMemColumnChunk::templateCopyValuesToPage<std::string, InMemOverflowFile*, PageByteCursor&,
    CopyDescription&>(arrow::Array& array, InMemOverflowFile* overflowFile,
    PageByteCursor& overflowCursor, CopyDescription& copyDesc) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::DATE: {
        templateCopyValuesAsStringToPage<date_t>(array);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        templateCopyValuesAsStringToPage<timestamp_t>(array);
    } break;
    case LogicalTypeID::INTERVAL: {
        templateCopyValuesAsStringToPage<interval_t>(array);
    } break;
    case LogicalTypeID::FIXED_LIST: {
        // Fixed list is a fixed-sized blob.
        templateCopyValuesAsStringToPage<uint8_t*, CopyDescription&>(array, copyDesc);
    } break;
    case LogicalTypeID::VAR_LIST: {
        templateCopyValuesAsStringToPage<ku_list_t, InMemOverflowFile*, PageByteCursor&,
            CopyDescription&>(array, overflowFile, overflowCursor, copyDesc);
    } break;
    case LogicalTypeID::STRING: {
        templateCopyValuesAsStringToPage<ku_string_t, InMemOverflowFile*, PageByteCursor&>(
            array, overflowFile, overflowCursor);
    } break;
    case LogicalTypeID::STRUCT: {
        templateCopyValuesAsStringToPage<struct_entry_t, InMemOverflowFile*, PageByteCursor&,
            CopyDescription&>(array, overflowFile, overflowCursor, copyDesc);
    } break;
    default: {
        throw CopyException(
            "Unsupported data type for InMemColumnChunk::templateCopyValuesAsStringToPage.");
    }
    }
}

// Bool
template<>
void InMemColumnChunk::setValueFromString<bool>(const char* value, uint64_t length, uint64_t pos) {
    std::istringstream boolStream{std::string(value)};
    bool booleanVal;
    boolStream >> std::boolalpha >> booleanVal;
    setValue(booleanVal, pos);
}

// String
template<>
void InMemColumnChunk::setValueFromString<ku_string_t, InMemOverflowFile*, PageByteCursor&>(
    const char* value, uint64_t length, uint64_t pos, InMemOverflowFile* overflowFile,
    PageByteCursor& overflowCursor) {
    if (length > BufferPoolConstants::PAGE_4KB_SIZE) {
        length = BufferPoolConstants::PAGE_4KB_SIZE;
    }
    auto val = overflowFile->copyString(value, length, overflowCursor);
    setValue(val, pos);
}

// Fixed list
template<>
void InMemColumnChunk::setValueFromString<uint8_t*, CopyDescription&>(
    const char* value, uint64_t length, uint64_t pos, CopyDescription& copyDescription) {
    auto fixedListVal =
        TableCopyExecutor::getArrowFixedList(value, 1, length - 2, dataType, copyDescription);
    // TODO(Guodong): Keep value size as a class field.
    memcpy(buffer.get() + pos * storage::StorageUtils::getDataTypeSize(dataType),
        fixedListVal.get(), storage::StorageUtils::getDataTypeSize(dataType));
}

// Var list
template<>
void InMemColumnChunk::setValueFromString<ku_list_t, InMemOverflowFile*, PageByteCursor&,
    CopyDescription&>(const char* value, uint64_t length, uint64_t pos,
    InMemOverflowFile* overflowFile, PageByteCursor& overflowCursor,
    CopyDescription& copyDescription) {
    auto varListVal =
        TableCopyExecutor::getArrowVarList(value, 1, length - 2, dataType, copyDescription);
    auto val = overflowFile->copyList(*varListVal, overflowCursor);
    setValue(val, pos);
}

// Interval
template<>
void InMemColumnChunk::setValueFromString<interval_t>(
    const char* value, uint64_t length, uint64_t pos) {
    auto val = Interval::FromCString(value, length);
    setValue(val, pos);
}

// Date
template<>
void InMemColumnChunk::setValueFromString<date_t>(
    const char* value, uint64_t length, uint64_t pos) {
    auto val = Date::FromCString(value, length);
    setValue(val, pos);
}

// Timestamp
template<>
void InMemColumnChunk::setValueFromString<timestamp_t>(
    const char* value, uint64_t length, uint64_t pos) {
    auto val = Timestamp::FromCString(value, length);
    setValue(val, pos);
}

// Struct
template<>
void InMemColumnChunk::setValueFromString<struct_entry_t, InMemOverflowFile*, PageByteCursor&,
    CopyDescription&>(const char* value, uint64_t length, uint64_t pos,
    InMemOverflowFile* overflowFile, PageByteCursor& overflowCursor,
    CopyDescription& copyDescription) {
    auto structFieldTypes = StructType::getStructFieldTypes(&dataType);
    auto structFieldNames = StructType::getStructFieldNames(&dataType);
    std::regex whiteSpacePattern{"\\s"};
    // Removes the leading and trailing '{', '}';
    auto structString = std::string(value, length).substr(1, length - 2);
    auto structFields = StringUtils::split(structString, ",");
    for (auto& structField : structFields) {
        auto delimPos = structField.find(':');
        auto structFieldName =
            std::regex_replace(structField.substr(0, delimPos), whiteSpacePattern, "");
        auto structFieldIdx =
            InMemStructColumnChunk::getStructFieldIdx(structFieldNames, structFieldName);
        auto structFieldValue = structField.substr(delimPos + 1);
        InMemStructColumnChunk::setValueToStructColumnField(childChunks[structFieldIdx].get(), pos,
            structFieldIdx, structFieldValue, *structFieldTypes[structFieldIdx]);
        childChunks[structFieldIdx]->getNullChunk()->setValue(false, pos);
    }
}

field_idx_t InMemStructColumnChunk::getStructFieldIdx(
    std::vector<std::string> structFieldNames, std::string structFieldName) {
    StringUtils::toUpper(structFieldName);
    auto it = std::find_if(structFieldNames.begin(), structFieldNames.end(),
        [structFieldName](const std::string& name) { return name == structFieldName; });
    if (it == structFieldNames.end()) {
        throw ConversionException{
            StringUtils::string_format("Invalid struct field name: {}.", structFieldName)};
    }
    return std::distance(structFieldNames.begin(), it);
}

void InMemStructColumnChunk::setValueToStructColumnField(InMemColumnChunk* chunk, offset_t pos,
    field_idx_t structFieldIdx, const std::string& structFieldValue, const LogicalType& dataType) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::INT64: {
        chunk->setValueFromString<int64_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::INT32: {
        chunk->setValueFromString<int32_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::INT16: {
        chunk->setValueFromString<int16_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::DOUBLE: {
        chunk->setValueFromString<double_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::FLOAT: {
        chunk->setValueFromString<float_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::BOOL: {
        chunk->setValueFromString<bool>(structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::DATE: {
        chunk->setValueFromString<date_t>(structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        chunk->setValueFromString<timestamp_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::INTERVAL: {
        chunk->setValueFromString<interval_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    default: {
        throw NotImplementedException{StringUtils::string_format(
            "Unsupported data type: {}.", LogicalTypeUtils::dataTypeToString(dataType))};
    }
    }
}

} // namespace storage
} // namespace kuzu
