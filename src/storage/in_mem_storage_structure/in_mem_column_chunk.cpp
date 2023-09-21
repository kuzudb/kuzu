#include "storage/in_mem_storage_structure/in_mem_column_chunk.h"

#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/exception/not_implemented.h"
#include "common/string_utils.h"
#include "common/types/blob.h"
#include "common/types/types.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

InMemColumnChunk::InMemColumnChunk(LogicalType dataType, offset_t startNodeOffset,
    offset_t endNodeOffset, std::unique_ptr<CSVReaderConfig> csvReaderConfig, bool requireNullBits)
    : dataType{std::move(dataType)}, startNodeOffset{startNodeOffset}, csvReaderConfig{std::move(
                                                                           csvReaderConfig)} {
    numBytesPerValue = getDataTypeSizeInColumn(this->dataType);
    numBytes = numBytesPerValue * (endNodeOffset - startNodeOffset + 1);
    buffer = std::make_unique<uint8_t[]>(numBytes);
    if (requireNullBits) {
        auto copyDescCloned = this->csvReaderConfig ? this->csvReaderConfig->copy() : nullptr;
        nullChunk = std::make_unique<InMemColumnChunk>(LogicalType{LogicalTypeID::BOOL},
            startNodeOffset, endNodeOffset, std::move(copyDescCloned), false /* hasNull */);
        memset(nullChunk->getData(), true, nullChunk->getNumBytes());
    }
}

void InMemColumnChunk::setValueAtPos(const uint8_t* val, offset_t pos) {
    memcpy(buffer.get() + getOffsetInBuffer(pos), val, numBytesPerValue);
    nullChunk->setValue(false, pos);
}

void InMemColumnChunk::flush(FileInfo* walFileInfo) {
    if (numBytes > 0) {
        auto startFileOffset = startNodeOffset * numBytesPerValue;
        FileUtils::writeToFile(walFileInfo, buffer.get(), numBytes, startFileOffset);
    }
}

template<typename ARROW_TYPE>
void InMemColumnChunk::templateCopyArrowStringArray(
    arrow::Array& array, arrow::Array* nodeOffsets) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::DATE: {
        templateCopyValuesAsStringToPage<date_t, ARROW_TYPE>(array, nodeOffsets);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        templateCopyValuesAsStringToPage<timestamp_t, ARROW_TYPE>(array, nodeOffsets);
    } break;
    case LogicalTypeID::INTERVAL: {
        templateCopyValuesAsStringToPage<interval_t, ARROW_TYPE>(array, nodeOffsets);
    } break;
    case LogicalTypeID::FIXED_LIST: {
        templateCopyValuesAsStringToPage<uint8_t*, ARROW_TYPE>(array, nodeOffsets);
    } break;
    default: {
        throw NotImplementedException{"InMemColumnChunk::templateCopyArrowStringArray"};
    }
    }
}

template<typename KU_TYPE, typename ARROW_TYPE>
void InMemColumnChunk::templateCopyValuesAsStringToPage(
    arrow::Array& array, arrow::Array* nodeOffsets) {
    auto& stringArray = (ARROW_TYPE&)array;
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
            setValueFromString<KU_TYPE>(value.data(), value.length(), offset);
            nullChunk->setValue<bool>(false, offset);
        }
    } else {
        for (auto i = 0u; i < stringArray.length(); i++) {
            auto value = stringArray.GetView(i);
            auto offset = offsetsInArray ? offsetsInArray[i] : i;
            setValueFromString<KU_TYPE>(value.data(), value.length(), offset);
            nullChunk->setValue<bool>(false, offset);
        }
    }
}

uint32_t InMemColumnChunk::getDataTypeSizeInColumn(LogicalType& dataType) {
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

void InMemColumnChunk::copyArrowArray(
    arrow::Array& arrowArray, PropertyCopyState* copyState, arrow::Array* nodeOffsets) {
    switch (arrowArray.type_id()) {
    case arrow::Type::BOOL: {
        templateCopyValuesToPage<bool>(arrowArray, nodeOffsets);
    } break;
    case arrow::Type::INT8: {
        templateCopyValuesToPage<int8_t>(arrowArray, nodeOffsets);
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
    case arrow::Type::UINT8: {
        templateCopyValuesToPage<uint8_t>(arrowArray, nodeOffsets);
    } break;
    case arrow::Type::UINT16: {
        templateCopyValuesToPage<uint16_t>(arrowArray, nodeOffsets);
    } break;
    case arrow::Type::UINT32: {
        templateCopyValuesToPage<uint32_t>(arrowArray, nodeOffsets);
    } break;
    case arrow::Type::UINT64: {
        templateCopyValuesToPage<uint64_t>(arrowArray, nodeOffsets);
    } break;
    case arrow::Type::DOUBLE: {
        templateCopyValuesToPage<double_t>(arrowArray, nodeOffsets);
    } break;
    case arrow::Type::FLOAT: {
        templateCopyValuesToPage<float_t>(arrowArray, nodeOffsets);
    } break;
    case arrow::Type::DATE32: {
        templateCopyValuesToPage<date_t>(arrowArray, nodeOffsets);
    } break;
    case arrow::Type::TIMESTAMP: {
        templateCopyValuesToPage<timestamp_t>(arrowArray, nodeOffsets);
    } break;
    case arrow::Type::FIXED_SIZE_LIST: {
        templateCopyValuesToPage<uint8_t*>(arrowArray, nodeOffsets);
    } break;
    case arrow::Type::STRING: {
        templateCopyArrowStringArray<arrow::StringArray>(arrowArray, nodeOffsets);
    } break;
    case arrow::Type::LARGE_STRING: {
        templateCopyArrowStringArray<arrow::LargeStringArray>(arrowArray, nodeOffsets);
    } break;
    default: {
        throw CopyException(
            StringUtils::string_format("Unsupported data type {}.", arrowArray.type()->ToString()));
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

void InMemColumnChunkWithOverflow::copyArrowArray(
    arrow::Array& array, PropertyCopyState* copyState, arrow::Array* nodeOffsets) {
    assert(array.type_id() == arrow::Type::STRING || array.type_id() == arrow::Type::LIST);
    // VAR_LIST is stored as strings in csv file whereas PARQUET natively supports LIST dataType.
    switch (array.type_id()) {
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING: {
        switch (dataType.getLogicalTypeID()) {
        case LogicalTypeID::STRING: {
            templateCopyArrowStringArray<ku_string_t>(array, copyState, nodeOffsets);
        } break;
        case LogicalTypeID::BLOB: {
            templateCopyArrowStringArray<blob_t>(array, copyState, nodeOffsets);
        } break;
        case LogicalTypeID::VAR_LIST: {
            templateCopyArrowStringArray<ku_list_t>(array, copyState, nodeOffsets);
        } break;
        default: {
            throw NotImplementedException{"InMemColumnChunkWithOverflow::copyArrowArray"};
        }
        }
    } break;
    case arrow::Type::LIST: {
        copyValuesToPageWithOverflow(array, copyState, nodeOffsets);
    } break;
    default:
        throw NotImplementedException{"InMemColumnChunkWithOverflow::copyArrowArray"};
    }
}

void InMemColumnChunkWithOverflow::copyValuesToPageWithOverflow(
    arrow::Array& array, PropertyCopyState* copyState, arrow::Array* nodeOffsets) {
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
            auto kuList =
                inMemOverflowFile->appendList(dataType, listArray, i, copyState->overflowCursor);
            auto offset = offsetsInArray ? offsetsInArray[i] : i;
            setValue(kuList, offset);
            nullChunk->setValue<bool>(false, offset);
        }
    } else {
        for (auto i = 0u; i < listArray.length(); i++) {
            auto kuList =
                inMemOverflowFile->appendList(dataType, listArray, i, copyState->overflowCursor);
            auto offset = offsetsInArray ? offsetsInArray[i] : i;
            setValue(kuList, offset);
            nullChunk->setValue<bool>(false, offset);
        }
    }
}

template<typename KU_TYPE>
void InMemColumnChunkWithOverflow::templateCopyArrowStringArray(
    arrow::Array& array, kuzu::storage::PropertyCopyState* copyState, arrow::Array* nodeOffsets) {
    switch (array.type_id()) {
    case arrow::Type::STRING: {
        templateCopyValuesAsStringToPageWithOverflow<KU_TYPE, arrow::StringArray>(
            array, copyState, nodeOffsets);
    } break;
    case arrow::Type::LARGE_STRING: {
        templateCopyValuesAsStringToPageWithOverflow<KU_TYPE, arrow::LargeStringArray>(
            array, copyState, nodeOffsets);
    } break;
    default: {
        throw NotImplementedException{"InMemColumnChunkWithOverflow::templateCopyArrowStringArray"};
    }
    }
}

template<typename KU_TYPE, typename ARROW_TYPE>
void InMemColumnChunkWithOverflow::templateCopyValuesAsStringToPageWithOverflow(
    arrow::Array& array, PropertyCopyState* copyState, arrow::Array* nodeOffsets) {
    auto& stringArray = (ARROW_TYPE&)array;
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
            setValWithOverflow<KU_TYPE>(
                copyState->overflowCursor, value.data(), value.length(), offset);
            nullChunk->setValue<bool>(false, offset);
        }
    } else {
        for (auto i = 0u; i < array.length(); i++) {
            auto value = stringArray.GetView(i);
            auto offset = offsetsInArray ? offsetsInArray[i] : i;
            setValWithOverflow<KU_TYPE>(
                copyState->overflowCursor, value.data(), value.length(), offset);
            nullChunk->setValue<bool>(false, offset);
        }
    }
}

// STRING
template<>
void InMemColumnChunkWithOverflow::setValWithOverflow<ku_string_t>(
    PageByteCursor& overflowCursor, const char* value, uint64_t length, uint64_t pos) {
    if (length > BufferPoolConstants::PAGE_4KB_SIZE) {
        throw CopyException(
            ExceptionMessage::overLargeStringValueException(std::to_string(length)));
    }
    auto val = inMemOverflowFile->copyString(value, length, overflowCursor);
    setValue(val, pos);
}

// BLOB
template<>
void InMemColumnChunkWithOverflow::setValWithOverflow<blob_t>(
    PageByteCursor& overflowCursor, const char* value, uint64_t length, uint64_t pos) {
    auto blobLen = Blob::fromString(
        value, std::min(length, BufferPoolConstants::PAGE_4KB_SIZE), blobBuffer.get());
    auto blobVal = inMemOverflowFile->copyString(
        reinterpret_cast<const char*>(blobBuffer.get()), blobLen, overflowCursor);
    setValue(blobVal, pos);
}

// VAR_LIST
template<>
void InMemColumnChunkWithOverflow::setValWithOverflow<ku_list_t>(
    PageByteCursor& overflowCursor, const char* value, uint64_t length, uint64_t pos) {
    auto varListVal =
        TableCopyUtils::getVarListValue(value, 1, length - 2, dataType, *csvReaderConfig);
    auto val = inMemOverflowFile->copyList(*varListVal, overflowCursor);
    setValue(val, pos);
}

InMemFixedListColumnChunk::InMemFixedListColumnChunk(LogicalType dataType, offset_t startNodeOffset,
    offset_t endNodeOffset, std::unique_ptr<CSVReaderConfig> csvReaderConfig)
    : InMemColumnChunk{
          std::move(dataType), startNodeOffset, endNodeOffset, std::move(csvReaderConfig)} {
    numElementsInAPage = PageUtils::getNumElementsInAPage(numBytesPerValue, false /* hasNull */);
    auto startNodeOffsetCursor =
        PageUtils::getPageByteCursorForPos(startNodeOffset, numElementsInAPage, numBytesPerValue);
    auto endNodeOffsetCursor =
        PageUtils::getPageByteCursorForPos(endNodeOffset, numElementsInAPage, numBytesPerValue);
    numBytes = (endNodeOffsetCursor.pageIdx - startNodeOffsetCursor.pageIdx) *
                   BufferPoolConstants::PAGE_4KB_SIZE +
               endNodeOffsetCursor.offsetInPage - startNodeOffsetCursor.offsetInPage +
               numBytesPerValue;
    buffer = std::make_unique<uint8_t[]>(numBytes);
}

void InMemFixedListColumnChunk::flush(FileInfo* walFileInfo) {
    if (numBytes > 0) {
        auto pageByteCursor = PageUtils::getPageByteCursorForPos(
            startNodeOffset, numElementsInAPage, numBytesPerValue);
        auto startFileOffset = pageByteCursor.pageIdx * BufferPoolConstants::PAGE_4KB_SIZE +
                               pageByteCursor.offsetInPage;
        FileUtils::writeToFile(walFileInfo, buffer.get(), numBytes, startFileOffset);
    }
}

offset_t InMemFixedListColumnChunk::getOffsetInBuffer(offset_t pos) {
    auto posCursor = PageUtils::getPageByteCursorForPos(
        pos + startNodeOffset, numElementsInAPage, numBytesPerValue);
    auto startNodeCursor =
        PageUtils::getPageByteCursorForPos(startNodeOffset, numElementsInAPage, numBytesPerValue);
    auto offsetInBuffer =
        (posCursor.pageIdx - startNodeCursor.pageIdx) * BufferPoolConstants::PAGE_4KB_SIZE +
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
        TableCopyUtils::getArrowFixedList(value, 1, length - 2, dataType, *csvReaderConfig);
    // TODO(Guodong): Keep value size as a class field.
    memcpy(buffer.get() + pos * storage::StorageUtils::getDataTypeSize(dataType),
        fixedListVal.get(), storage::StorageUtils::getDataTypeSize(dataType));
}

// Interval
template<>
void InMemColumnChunk::setValueFromString<interval_t>(
    const char* value, uint64_t length, uint64_t pos) {
    auto val = Interval::fromCString(value, length);
    setValue(val, pos);
}

// Date
template<>
void InMemColumnChunk::setValueFromString<date_t>(
    const char* value, uint64_t length, uint64_t pos) {
    auto val = Date::fromCString(value, length);
    setValue(val, pos);
}

// Timestamp
template<>
void InMemColumnChunk::setValueFromString<timestamp_t>(
    const char* value, uint64_t length, uint64_t pos) {
    auto val = Timestamp::fromCString(value, length);
    setValue(val, pos);
}

} // namespace storage
} // namespace kuzu
