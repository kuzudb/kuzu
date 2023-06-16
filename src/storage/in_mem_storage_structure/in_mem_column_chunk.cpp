#include "storage/in_mem_storage_structure/in_mem_column_chunk.h"

#include "common/string_utils.h"
#include "common/types/types.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

InMemColumnChunk::InMemColumnChunk(LogicalType dataType, offset_t startNodeOffset,
    offset_t endNodeOffset, const common::CopyDescription* copyDescription, bool requireNullBits)
    : dataType{std::move(dataType)}, startNodeOffset{startNodeOffset}, copyDescription{
                                                                           copyDescription} {
    numBytesPerValue = getDataTypeSizeInColumn(this->dataType);
    numBytes = numBytesPerValue * (endNodeOffset - startNodeOffset + 1);
    buffer = std::make_unique<uint8_t[]>(numBytes);
    if (requireNullBits) {
        nullChunk = std::make_unique<InMemColumnChunk>(LogicalType{LogicalTypeID::BOOL},
            startNodeOffset, endNodeOffset, copyDescription, false /* hasNull */);
        memset(nullChunk->getData(), true, nullChunk->getNumBytes());
    }
}

void InMemColumnChunk::setValueAtPos(const uint8_t* val, common::offset_t pos) {
    memcpy(buffer.get() + getOffsetInBuffer(pos), val, numBytesPerValue);
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

void InMemColumnChunk::copyArrowBatch(std::shared_ptr<arrow::RecordBatch> batch) {
    assert(batch->num_columns() == 1);
    copyArrowArray(*batch->column(0), nullptr /* nodeOffsets */);
}

void InMemColumnChunk::copyArrowArray(arrow::Array& arrowArray, arrow::Array* nodeOffsets) {
    switch (arrowArray.type_id()) {
    case arrow::Type::BOOL: {
        templateCopyValuesToPage<bool>(arrowArray, nodeOffsets);
    } break;
    case arrow::Type::INT16: {
        templateCopyValuesToPage<int16_t>(arrowArray, nodeOffsets);
    } break;
    case arrow::Type::INT32: {
        templateCopyValuesToPage<int32_t>(arrowArray, nodeOffsets);
    } break;
    case arrow::Type::INT64: {
        templateCopyValuesToPage<int64_t>(arrowArray, nodeOffsets);
    } break;
    case arrow::Type::DOUBLE: {
        templateCopyValuesToPage<double_t>(arrowArray, nodeOffsets);
    } break;
    case arrow::Type::FLOAT: {
        templateCopyValuesToPage<float_t>(arrowArray, nodeOffsets);
    } break;
    case arrow::Type::DATE32: {
        templateCopyValuesToPage<common::date_t>(arrowArray, nodeOffsets);
    } break;
    case arrow::Type::TIMESTAMP: {
        templateCopyValuesToPage<common::timestamp_t>(arrowArray, nodeOffsets);
    } break;
    case arrow::Type::FIXED_SIZE_LIST: {
        templateCopyValuesToPage<uint8_t*>(arrowArray, nodeOffsets);
    } break;
    case arrow::Type::STRING: {
        switch (dataType.getLogicalTypeID()) {
        case LogicalTypeID::DATE: {
            templateCopyValuesAsStringToPage<date_t>(arrowArray, nodeOffsets);
        } break;
        case LogicalTypeID::TIMESTAMP: {
            templateCopyValuesAsStringToPage<timestamp_t>(arrowArray, nodeOffsets);
        } break;
        case LogicalTypeID::INTERVAL: {
            templateCopyValuesAsStringToPage<interval_t>(arrowArray, nodeOffsets);
        } break;
        case LogicalTypeID::FIXED_LIST: {
            // Fixed list is a fixed-sized blob.
            templateCopyValuesAsStringToPage<uint8_t*>(arrowArray, nodeOffsets);
        } break;
        default: {
            throw common::CopyException("Unsupported data type ");
        }
        }
    } break;
    default: {
        throw common::CopyException("Unsupported data type " + arrowArray.type()->ToString());
    }
    }
}

template<typename T>
void InMemColumnChunk::templateCopyValuesToPage(arrow::Array& array, arrow::Array* nodeOffsets) {
    const auto& arrayData = array.data();
    auto valuesInArray = arrayData->GetValues<T>(1 /* value buffer */);
    auto valuesInChunk = (T*)(buffer.get());
    const offset_t* offsetsInArray =
        nodeOffsets == nullptr ? nullptr :
                                 nodeOffsets->data()->GetValues<offset_t>(1 /* value buffer */);
    if (arrayData->MayHaveNulls()) {
        for (auto i = 0u; i < array.length(); i++) {
            if (arrayData->IsNull(i)) {
                continue;
            }
            auto offset = offsetsInArray ? offsetsInArray[i] : i;
            valuesInChunk[offset] = valuesInArray[i];
            nullChunk->setValue<bool>(false, offset);
        }
    } else {
        for (auto i = 0u; i < array.length(); i++) {
            auto offset = offsetsInArray ? offsetsInArray[i] : i;
            valuesInChunk[offset] = valuesInArray[i];
            nullChunk->setValue<bool>(false, offset);
        }
    }
}

template<>
void InMemColumnChunk::templateCopyValuesToPage<bool>(
    arrow::Array& array, arrow::Array* nodeOffsets) {
    auto& boolArray = (arrow::BooleanArray&)array;
    auto data = boolArray.data();
    auto valuesInChunk = (bool*)(buffer.get());
    const offset_t* offsetsInArray =
        nodeOffsets == nullptr ? nullptr :
                                 nodeOffsets->data()->GetValues<offset_t>(1 /* value buffer */);
    if (data->MayHaveNulls()) {
        for (auto i = 0u; i < boolArray.length(); i++) {
            if (data->IsNull(i)) {
                continue;
            }
            auto offset = offsetsInArray ? offsetsInArray[i] : i;
            valuesInChunk[offset] = boolArray.Value(i);
            nullChunk->setValue<bool>(false, offset);
        }
    } else {
        for (auto i = 0u; i < boolArray.length(); i++) {
            auto offset = offsetsInArray ? offsetsInArray[i] : i;
            valuesInChunk[offset] = boolArray.Value(i);
            nullChunk->setValue<bool>(false, offset);
        }
    }
}

template<>
void InMemColumnChunk::templateCopyValuesToPage<uint8_t*>(
    arrow::Array& array, arrow::Array* nodeOffsets) {
    auto& listArray = (arrow::FixedSizeListArray&)array;
    auto childTypeSize =
        storage::StorageUtils::getDataTypeSize(*FixedListType::getChildType(&dataType));
    auto listDataBuffer = listArray.values()->data()->buffers[1]->data();
    auto data = listArray.data();
    const offset_t* offsetsInArray =
        nodeOffsets == nullptr ? nullptr :
                                 nodeOffsets->data()->GetValues<offset_t>(1 /* value buffer */);
    if (data->MayHaveNulls()) {
        for (auto i = 0u; i < listArray.length(); i++) {
            if (data->IsNull(i)) {
                continue;
            }
            auto offset = offsetsInArray ? offsetsInArray[i] : i;
            setValueAtPos(listDataBuffer + listArray.value_offset(i) * childTypeSize, offset);
        }
    } else {
        for (auto i = 0u; i < listArray.length(); i++) {
            auto offset = offsetsInArray ? offsetsInArray[i] : i;
            setValueAtPos(listDataBuffer + listArray.value_offset(i) * childTypeSize, offset);
        }
    }
}

template<typename T>
void InMemColumnChunk::templateCopyValuesAsStringToPage(
    arrow::Array& array, arrow::Array* nodeOffsets) {
    auto& stringArray = (arrow::StringArray&)array;
    auto arrayData = stringArray.data();
    const offset_t* offsetsInArray =
        nodeOffsets == nullptr ? nullptr :
                                 nodeOffsets->data()->GetValues<offset_t>(1 /* value buffer */);
    if (arrayData->MayHaveNulls()) {
        for (auto i = 0u; i < stringArray.length(); i++) {
            if (arrayData->IsNull(i)) {
                continue;
            }
            auto value = stringArray.GetView(i);
            auto offset = offsetsInArray ? offsetsInArray[i] : i;
            setValueFromString<T>(value.data(), value.length(), offset);
            nullChunk->setValue<bool>(false, offset);
        }
    } else {
        for (auto i = 0u; i < stringArray.length(); i++) {
            auto value = stringArray.GetView(i);
            auto offset = offsetsInArray ? offsetsInArray[i] : i;
            setValueFromString<T>(value.data(), value.length(), offset);
            nullChunk->setValue<bool>(false, offset);
        }
    }
}

void InMemColumnChunkWithOverflow::copyArrowArray(arrow::Array& array, arrow::Array* nodeOffsets) {
    assert(array.type_id() == arrow::Type::STRING || array.type_id() == arrow::Type::LIST);
    // VAR_LIST is stored as strings in csv file whereas PARQUET natively supports LIST dataType.
    switch (array.type_id()) {
    case arrow::Type::STRING: {
        switch (dataType.getLogicalTypeID()) {
        case common::LogicalTypeID::STRING: {
            templateCopyValuesAsStringToPageWithOverflow<ku_string_t>(array, nodeOffsets);
        } break;
        case common::LogicalTypeID::BLOB: {
            templateCopyValuesAsStringToPageWithOverflow<blob_t>(array, nodeOffsets);
        } break;
        case common::LogicalTypeID::VAR_LIST: {
            templateCopyValuesAsStringToPageWithOverflow<ku_list_t>(array, nodeOffsets);
        } break;
        default: {
            throw NotImplementedException{"InMemColumnChunkWithOverflow::copyArrowArray"};
        }
        }
    } break;
    case arrow::Type::LIST: {
        copyValuesToPageWithOverflow(array, nodeOffsets);
    } break;
    default:
        throw NotImplementedException{"InMemColumnChunkWithOverflow::copyArrowArray"};
    }
}

template<typename T>
void InMemColumnChunkWithOverflow::templateCopyValuesAsStringToPageWithOverflow(
    arrow::Array& array, arrow::Array* nodeOffsets) {
    auto& stringArray = (arrow::StringArray&)array;
    auto arrayData = stringArray.data();
    const offset_t* offsetsInArray =
        nodeOffsets == nullptr ? nullptr :
                                 nodeOffsets->data()->GetValues<offset_t>(1 /* value buffer */);
    if (arrayData->MayHaveNulls()) {
        for (auto i = 0u; i < array.length(); i++) {
            if (arrayData->IsNull(i)) {
                continue;
            }
            auto value = stringArray.GetView(i);
            auto offset = offsetsInArray ? offsetsInArray[i] : i;
            setValWithOverflow<T>(value.data(), value.length(), offset);
            nullChunk->setValue<bool>(false, offset);
        }
    } else {
        for (auto i = 0u; i < array.length(); i++) {
            auto value = stringArray.GetView(i);
            auto offset = offsetsInArray ? offsetsInArray[i] : i;
            setValWithOverflow<T>(value.data(), value.length(), offset);
            nullChunk->setValue<bool>(false, offset);
        }
    }
}

void InMemColumnChunkWithOverflow::copyValuesToPageWithOverflow(
    arrow::Array& array, arrow::Array* nodeOffsets) {
    assert(array.type_id() == arrow::Type::LIST);
    auto& listArray = reinterpret_cast<arrow::ListArray&>(array);
    auto listArrayData = listArray.data();
    auto offsetsInArray = nodeOffsets == nullptr ?
                              nullptr :
                              nodeOffsets->data()->GetValues<offset_t>(1 /* value buffer */);
    if (listArray.data()->MayHaveNulls()) {
        for (auto i = 0u; i < listArray.length(); i++) {
            if (listArrayData->IsNull(i)) {
                continue;
            }
            auto kuList = inMemOverflowFile->appendList(dataType, listArray, i, overflowCursor);
            auto offset = offsetsInArray ? offsetsInArray[i] : i;
            setValue(kuList, offset);
            nullChunk->setValue<bool>(false, offset);
        }
    } else {
        for (auto i = 0u; i < listArray.length(); i++) {
            auto kuList = inMemOverflowFile->appendList(dataType, listArray, i, overflowCursor);
            auto offset = offsetsInArray ? offsetsInArray[i] : i;
            setValue(kuList, offset);
            nullChunk->setValue<bool>(false, offset);
        }
    }
}

// STRING
template<>
void InMemColumnChunkWithOverflow::setValWithOverflow<ku_string_t>(
    const char* value, uint64_t length, uint64_t pos) {
    if (length > BufferPoolConstants::PAGE_4KB_SIZE) {
        length = BufferPoolConstants::PAGE_4KB_SIZE;
    }
    auto val = inMemOverflowFile->copyString(value, length, overflowCursor);
    setValue(val, pos);
}

// BLOB
template<>
void InMemColumnChunkWithOverflow::setValWithOverflow<blob_t>(
    const char* value, uint64_t length, uint64_t pos) {
    auto blobLen = Blob::fromString(
        value, std::min(length, BufferPoolConstants::PAGE_4KB_SIZE), blobBuffer.get());
    auto blobVal = inMemOverflowFile->copyString(
        reinterpret_cast<const char*>(blobBuffer.get()), blobLen, overflowCursor);
    setValue(blobVal, pos);
}

// VAR_LIST
template<>
void InMemColumnChunkWithOverflow::setValWithOverflow<ku_list_t>(
    const char* value, uint64_t length, uint64_t pos) {
    auto varListVal =
        TableCopyUtils::getArrowVarList(value, 1, length - 2, dataType, *copyDescription);
    auto val = inMemOverflowFile->copyList(*varListVal, overflowCursor);
    setValue(val, pos);
}

InMemStructColumnChunk::InMemStructColumnChunk(common::LogicalType dataType,
    common::offset_t startNodeOffset, common::offset_t endNodeOffset,
    const common::CopyDescription* copyDescription)
    : InMemColumnChunk{std::move(dataType), startNodeOffset, endNodeOffset, copyDescription} {}

void InMemStructColumnChunk::copyArrowArray(arrow::Array& array, arrow::Array* nodeOffsets) {
    auto offsetsInArray = nodeOffsets == nullptr ?
                              nullptr :
                              nodeOffsets->data()->GetValues<offset_t>(1 /* value buffer */);
    switch (array.type_id()) {
    case arrow::Type::STRING: {
        auto& stringArray = (arrow::StringArray&)array;
        auto arrayData = stringArray.data();
        if (arrayData->MayHaveNulls()) {
            for (auto i = 0u; i < stringArray.length(); i++) {
                if (arrayData->IsNull(i)) {
                    continue;
                }
                auto value = stringArray.GetView(i);
                auto offset = offsetsInArray ? offsetsInArray[i] : i;
                setStructFields(value.data(), value.length(), offset);
                nullChunk->setValue<bool>(false, offset);
            }
        } else {
            for (auto i = 0u; i < stringArray.length(); i++) {
                auto value = stringArray.GetView(i);
                auto offset = offsetsInArray ? offsetsInArray[i] : i;
                setStructFields(value.data(), value.length(), offset);
                nullChunk->setValue<bool>(false, offset);
            }
        }
    } break;
    case arrow::Type::STRUCT: {
        auto& structArray = (arrow::StructArray&)array;
        auto arrayData = structArray.data();
        if (common::StructType::getNumFields(&dataType) != structArray.type()->fields().size()) {
            throw CopyException{"Unmatched number of struct fields."};
        }
        for (auto i = 0u; i < structArray.num_fields(); i++) {
            auto fieldName = structArray.type()->fields()[i]->name();
            auto fieldIdx = common::StructType::getFieldIdx(&dataType, fieldName);
            if (fieldIdx == INVALID_STRUCT_FIELD_IDX) {
                throw CopyException{"Unmatched struct field name: " + fieldName + "."};
            }
            fieldChunks[fieldIdx]->copyArrowArray(*structArray.field(i), nodeOffsets);
        }
        for (auto i = 0u; i < structArray.length(); i++) {
            if (arrayData->IsNull(i)) {
                continue;
            }
            auto offset = offsetsInArray ? offsetsInArray[i] : i;
            nullChunk->setValue<bool>(false, offset);
        }
    } break;
    default: {
        throw common::NotImplementedException{"InMemStructColumnChunk::copyArrowArray"};
    }
    }
}

void InMemStructColumnChunk::setStructFields(const char* value, uint64_t length, uint64_t pos) {
    // Removes the leading and trailing '{', '}';
    auto structString = std::string(value, length).substr(1, length - 2);
    auto structFieldIdxAndValuePairs = parseStructFieldNameAndValues(dataType, structString);
    for (auto& fieldIdxAndValue : structFieldIdxAndValuePairs) {
        setValueToStructField(pos, fieldIdxAndValue.fieldValue, fieldIdxAndValue.fieldIdx);
    }
}

void InMemStructColumnChunk::setValueToStructField(
    offset_t pos, const std::string& structFieldValue, struct_field_idx_t structFiledIdx) {
    auto fieldChunk = fieldChunks[structFiledIdx].get();
    fieldChunk->getNullChunk()->setValue(false, pos);
    switch (fieldChunk->getDataType().getLogicalTypeID()) {
    case LogicalTypeID::INT64: {
        fieldChunk->setValueFromString<int64_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::INT32: {
        fieldChunk->setValueFromString<int32_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::INT16: {
        fieldChunk->setValueFromString<int16_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::DOUBLE: {
        fieldChunk->setValueFromString<double_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::FLOAT: {
        fieldChunk->setValueFromString<float_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::BOOL: {
        fieldChunk->setValueFromString<bool>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::DATE: {
        fieldChunk->setValueFromString<date_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        fieldChunk->setValueFromString<timestamp_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::INTERVAL: {
        fieldChunk->setValueFromString<interval_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::STRING: {
        reinterpret_cast<InMemColumnChunkWithOverflow*>(fieldChunk)
            ->setValWithOverflow<ku_string_t>(
                structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::VAR_LIST: {
        reinterpret_cast<InMemColumnChunkWithOverflow*>(fieldChunk)
            ->setValWithOverflow<ku_list_t>(
                structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::STRUCT: {
        reinterpret_cast<InMemStructColumnChunk*>(fieldChunk)
            ->setStructFields(structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    default: {
        throw NotImplementedException{StringUtils::string_format(
            "Unsupported data type: {}.", LogicalTypeUtils::dataTypeToString(dataType))};
    }
    }
}

std::vector<StructFieldIdxAndValue> InMemStructColumnChunk::parseStructFieldNameAndValues(
    common::LogicalType& type, const std::string& structString) {
    std::vector<StructFieldIdxAndValue> structFieldIdxAndValueParis;
    uint64_t curPos = 0u;
    while (curPos < structString.length()) {
        auto fieldName = parseStructFieldName(structString, curPos);
        auto fieldIdx = common::StructType::getFieldIdx(&type, fieldName);
        if (fieldIdx == INVALID_STRUCT_FIELD_IDX) {
            throw ParserException{"Invalid struct field name: " + fieldName};
        }
        auto structFieldValue = parseStructFieldValue(structString, curPos);
        structFieldIdxAndValueParis.emplace_back(fieldIdx, structFieldValue);
    }
    return structFieldIdxAndValueParis;
}

std::string InMemStructColumnChunk::parseStructFieldName(
    const std::string& structString, uint64_t& curPos) {
    auto startPos = curPos;
    while (curPos < structString.length()) {
        if (structString[curPos] == ':') {
            auto structFieldName = structString.substr(startPos, curPos - startPos);
            StringUtils::removeWhiteSpaces(structFieldName);
            curPos++;
            return structFieldName;
        }
        curPos++;
    }
    throw ParserException{"Invalid struct string: " + structString};
}

std::string InMemStructColumnChunk::parseStructFieldValue(
    const std::string& structString, uint64_t& curPos) {
    auto numListBeginChars = 0u;
    auto numStructBeginChars = 0u;
    auto numDoubleQuotes = 0u;
    auto numSingleQuotes = 0u;
    // Skip leading white spaces.
    while (structString[curPos] == ' ') {
        curPos++;
    }
    auto startPos = curPos;
    while (curPos < structString.length()) {
        auto curChar = structString[curPos];
        if (curChar == '{') {
            numStructBeginChars++;
        } else if (curChar == '}') {
            numStructBeginChars--;
        } else if (curChar == copyDescription->csvReaderConfig->listBeginChar) {
            numListBeginChars++;
        } else if (curChar == copyDescription->csvReaderConfig->listEndChar) {
            numListBeginChars--;
        } else if (curChar == '"') {
            numDoubleQuotes ^= 1;
        } else if (curChar == '\'') {
            numSingleQuotes ^= 1;
        } else if (curChar == ',') {
            if (numListBeginChars == 0 && numStructBeginChars == 0 && numDoubleQuotes == 0 &&
                numSingleQuotes == 0) {
                curPos++;
                return structString.substr(startPos, curPos - startPos - 1);
            }
        }
        curPos++;
    }
    if (numListBeginChars == 0 && numStructBeginChars == 0 && numDoubleQuotes == 0 &&
        numSingleQuotes == 0) {
        return structString.substr(startPos, curPos - startPos);
    } else {
        throw common::ParserException{"Invalid struct string: " + structString};
    }
}

InMemFixedListColumnChunk::InMemFixedListColumnChunk(common::LogicalType dataType,
    common::offset_t startNodeOffset, common::offset_t endNodeOffset,
    const common::CopyDescription* copyDescription)
    : InMemColumnChunk{std::move(dataType), startNodeOffset, endNodeOffset, copyDescription} {
    numElementsInAPage = PageUtils::getNumElementsInAPage(numBytesPerValue, false /* hasNull */);
    auto startNodeOffsetCursor =
        PageUtils::getPageByteCursorForPos(startNodeOffset, numElementsInAPage, numBytesPerValue);
    auto endNodeOffsetCursor =
        PageUtils::getPageByteCursorForPos(endNodeOffset, numElementsInAPage, numBytesPerValue);
    numBytes = (endNodeOffsetCursor.pageIdx - startNodeOffsetCursor.pageIdx) *
                   common::BufferPoolConstants::PAGE_4KB_SIZE +
               endNodeOffsetCursor.offsetInPage - startNodeOffsetCursor.offsetInPage +
               numBytesPerValue;
    buffer = std::make_unique<uint8_t[]>(numBytes);
}

void InMemFixedListColumnChunk::flush(common::FileInfo* walFileInfo) {
    if (numBytes > 0) {
        auto pageByteCursor = PageUtils::getPageByteCursorForPos(
            startNodeOffset, numElementsInAPage, numBytesPerValue);
        auto startFileOffset = pageByteCursor.pageIdx * common::BufferPoolConstants::PAGE_4KB_SIZE +
                               pageByteCursor.offsetInPage;
        common::FileUtils::writeToFile(walFileInfo, buffer.get(), numBytes, startFileOffset);
    }
}

common::offset_t InMemFixedListColumnChunk::getOffsetInBuffer(common::offset_t pos) {
    auto posCursor = PageUtils::getPageByteCursorForPos(
        pos + startNodeOffset, numElementsInAPage, numBytesPerValue);
    auto startNodeCursor =
        PageUtils::getPageByteCursorForPos(startNodeOffset, numElementsInAPage, numBytesPerValue);
    auto offsetInBuffer =
        (posCursor.pageIdx - startNodeCursor.pageIdx) * common::BufferPoolConstants::PAGE_4KB_SIZE +
        posCursor.offsetInPage - startNodeCursor.offsetInPage;
    assert(offsetInBuffer + numBytesPerValue <= numBytes);
    return offsetInBuffer;
}

// Bool
template<>
void InMemColumnChunk::setValueFromString<bool>(const char* value, uint64_t length, uint64_t pos) {
    std::istringstream boolStream{std::string(value)};
    bool booleanVal;
    boolStream >> std::boolalpha >> booleanVal;
    setValue(booleanVal, pos);
}

// Fixed list
template<>
void InMemColumnChunk::setValueFromString<uint8_t*>(
    const char* value, uint64_t length, uint64_t pos) {
    auto fixedListVal =
        TableCopyUtils::getArrowFixedList(value, 1, length - 2, dataType, *copyDescription);
    // TODO(Guodong): Keep value size as a class field.
    memcpy(buffer.get() + pos * storage::StorageUtils::getDataTypeSize(dataType),
        fixedListVal.get(), storage::StorageUtils::getDataTypeSize(dataType));
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

} // namespace storage
} // namespace kuzu
