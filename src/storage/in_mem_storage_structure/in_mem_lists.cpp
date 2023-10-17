#include "storage/in_mem_storage_structure/in_mem_lists.h"

#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/types/blob.h"
#include "storage/in_mem_storage_structure/in_mem_column_chunk.h"
#include "storage/store/table_copy_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

PageElementCursor InMemLists::calcPageElementCursor(
    uint64_t reversePos, uint8_t numBytesPerElement, offset_t nodeOffset) {
    PageElementCursor cursor;
    auto listSize = listHeadersBuilder->getListSize(nodeOffset);
    auto csrOffset = listHeadersBuilder->getCSROffset(nodeOffset);
    auto pos = listSize - reversePos;
    cursor = PageUtils::getPageElementCursorForPos(csrOffset + pos, numElementsInAPage);
    cursor.pageIdx = listsMetadataBuilder->getPageMapperForChunkIdx(
        StorageUtils::getListChunkIdx(nodeOffset))((csrOffset + pos) / numElementsInAPage);
    return cursor;
}

InMemLists::InMemLists(std::string fName, uint64_t numBytesForElement, LogicalType dataType,
    uint64_t numNodes, std::unique_ptr<CSVReaderConfig> csvReaderConfig, bool hasNullBytes)
    : fName{std::move(fName)}, dataType{std::move(dataType)},
      numBytesForElement{numBytesForElement}, csvReaderConfig{std::move(csvReaderConfig)} {
    listsMetadataBuilder = make_unique<ListsMetadataBuilder>(this->fName);
    auto numChunks = StorageUtils::getListChunkIdx(numNodes);
    if (0 != (numNodes & (ListsMetadataConstants::LISTS_CHUNK_SIZE - 1))) {
        numChunks++;
    }
    listsMetadataBuilder->initChunkPageLists(numChunks);
    numElementsInAPage = PageUtils::getNumElementsInAPage(numBytesForElement, hasNullBytes);
    inMemFile = make_unique<InMemFile>(this->fName, numBytesForElement, hasNullBytes);
}

void InMemLists::copyArrowArray(arrow::Array* boundNodeOffsets, arrow::Array* posInRelLists,
    arrow::Array* array, PropertyCopyState* copyState) {
    switch (array->type_id()) {
    case arrow::Type::BOOL: {
        templateCopyArrayToRelLists<bool>(boundNodeOffsets, posInRelLists, array);
    } break;
    case arrow::Type::INT8: {
        templateCopyArrayToRelLists<int8_t>(boundNodeOffsets, posInRelLists, array);
    } break;
    case arrow::Type::INT16: {
        templateCopyArrayToRelLists<int16_t>(boundNodeOffsets, posInRelLists, array);
    } break;
    case arrow::Type::INT32: {
        templateCopyArrayToRelLists<int32_t>(boundNodeOffsets, posInRelLists, array);
    } break;
    case arrow::Type::INT64: {
        templateCopyArrayToRelLists<int64_t>(boundNodeOffsets, posInRelLists, array);
    } break;
    case arrow::Type::UINT8: {
        templateCopyArrayToRelLists<uint8_t>(boundNodeOffsets, posInRelLists, array);
    } break;
    case arrow::Type::UINT16: {
        templateCopyArrayToRelLists<uint16_t>(boundNodeOffsets, posInRelLists, array);
    } break;
    case arrow::Type::UINT32: {
        templateCopyArrayToRelLists<uint32_t>(boundNodeOffsets, posInRelLists, array);
    } break;
    case arrow::Type::UINT64: {
        templateCopyArrayToRelLists<uint64_t>(boundNodeOffsets, posInRelLists, array);
    } break;
    case arrow::Type::DOUBLE: {
        templateCopyArrayToRelLists<double_t>(boundNodeOffsets, posInRelLists, array);
    } break;
    case arrow::Type::FLOAT: {
        templateCopyArrayToRelLists<float_t>(boundNodeOffsets, posInRelLists, array);
    } break;
    case arrow::Type::DATE32: {
        templateCopyArrayToRelLists<date_t>(boundNodeOffsets, posInRelLists, array);
    } break;
    case arrow::Type::TIMESTAMP: {
        templateCopyArrayToRelLists<timestamp_t>(boundNodeOffsets, posInRelLists, array);
    } break;
    case arrow::Type::STRING: {
        templateCopyArrowStringArray<arrow::StringArray>(boundNodeOffsets, posInRelLists, array);
    } break;
    case arrow::Type::LARGE_STRING: {
        templateCopyArrowStringArray<arrow::LargeStringArray>(
            boundNodeOffsets, posInRelLists, array);
    } break;
        // TODO(Ziyi): Add support of VAR_LIST and more native parquet data types.
    default: {
        throw CopyException("Unsupported data type");
    }
    }
}

template<typename T>
void InMemLists::templateCopyArrayToRelLists(
    arrow::Array* boundNodeOffsets, arrow::Array* posInRelList, arrow::Array* array) {
    auto valuesInArray = array->data()->GetValues<T>(1 /* value buffer */);
    auto offsets = boundNodeOffsets->data()->GetValues<offset_t>(1);
    auto positions = posInRelList->data()->GetValues<int64_t>(1);
    if (array->data()->MayHaveNulls()) {
        for (auto i = 0u; i < array->length(); i++) {
            if (array->IsNull(i)) {
                continue;
            }
            setValue(offsets[i], positions[i], (uint8_t*)&valuesInArray[i]);
        }
    } else {
        for (auto i = 0u; i < array->length(); i++) {
            setValue(offsets[i], positions[i], (uint8_t*)&valuesInArray[i]);
        }
    }
}

template<>
void InMemLists::templateCopyArrayToRelLists<bool>(
    arrow::Array* boundNodeOffsets, arrow::Array* posInRelList, arrow::Array* array) {
    auto boolArray = (arrow::BooleanArray*)array;
    auto offsets = boundNodeOffsets->data()->GetValues<offset_t>(1);
    auto positions = posInRelList->data()->GetValues<int64_t>(1);
    if (array->data()->MayHaveNulls()) {
        for (auto i = 0u; i < array->length(); i++) {
            if (array->IsNull(i)) {
                continue;
            }
            bool val = boolArray->Value(i);
            setValue(offsets[i], positions[i], (uint8_t*)&val);
        }
    } else {
        for (auto i = 0u; i < array->length(); i++) {
            bool val = boolArray->Value(i);
            setValue(offsets[i], positions[i], (uint8_t*)&val);
        }
    }
}

void InMemLists::fillWithDefaultVal(
    uint8_t* defaultVal, uint64_t numNodes, ListHeaders* listHeaders) {
    PageByteCursor pageByteCursor{};
    auto fillInMemListsFunc = getFillInMemListsFunc(dataType);
    for (auto i = 0; i < numNodes; i++) {
        auto numElementsInList = listHeaders->getListSize(i);
        for (auto j = 0u; j < numElementsInList; j++) {
            fillInMemListsFunc(
                this, defaultVal, pageByteCursor, i, numElementsInList - j, dataType);
        }
    }
}

void InMemLists::saveToFile() {
    listsMetadataBuilder->saveToDisk();
    inMemFile->flush();
}

void InMemLists::setValue(offset_t nodeOffset, uint64_t pos, uint8_t* val) {
    auto cursor = calcPageElementCursor(pos, numBytesForElement, nodeOffset);
    inMemFile->getPage(cursor.pageIdx)
        ->write(cursor.elemPosInPage * numBytesForElement, cursor.elemPosInPage, val,
            numBytesForElement);
}

template<typename T>
void InMemLists::setValueFromString(
    offset_t nodeOffset, uint64_t pos, const char* val, uint64_t length) {
    auto numericVal = function::castStringToNum<T>(val, length);
    setValue(nodeOffset, pos, (uint8_t*)&numericVal);
}

template<>
void InMemLists::setValueFromString<bool>(
    offset_t nodeOffset, uint64_t pos, const char* val, uint64_t length) {
    std::istringstream boolStream{std::string(val)};
    bool booleanVal;
    boolStream >> std::boolalpha >> booleanVal;
    setValue(nodeOffset, pos, (uint8_t*)&booleanVal);
}

// Fixed list
template<>
void InMemLists::setValueFromString<uint8_t*>(
    offset_t nodeOffset, uint64_t pos, const char* val, uint64_t length) {
    auto fixedListVal =
        TableCopyUtils::getArrowFixedList(val, 1, length - 2, dataType, *csvReaderConfig);
    setValue(nodeOffset, pos, fixedListVal.get());
}

// Interval
template<>
void InMemLists::setValueFromString<interval_t>(
    offset_t nodeOffset, uint64_t pos, const char* val, uint64_t length) {
    auto intervalVal = Interval::fromCString(val, length);
    setValue(nodeOffset, pos, (uint8_t*)&intervalVal);
}

// Date
template<>
void InMemLists::setValueFromString<date_t>(
    offset_t nodeOffset, uint64_t pos, const char* val, uint64_t length) {
    auto dateVal = Date::fromCString(val, length);
    setValue(nodeOffset, pos, (uint8_t*)&dateVal);
}

// Timestamp
template<>
void InMemLists::setValueFromString<timestamp_t>(
    offset_t nodeOffset, uint64_t pos, const char* val, uint64_t length) {
    auto timestampVal = Timestamp::fromCString(val, length);
    setValue(nodeOffset, pos, (uint8_t*)&timestampVal);
}

void InMemLists::initListsMetadataAndAllocatePages(
    uint64_t numNodes, ListHeaders* listHeaders, ListsMetadata* listsMetadata) {
    offset_t nodeOffset = 0u;
    auto largeListIdx = 0u;
    auto numElementsPerPage =
        PageUtils::getNumElementsInAPage(numBytesForElement, true /* hasNull */);
    auto numChunks = StorageUtils::getNumChunks(numNodes);
    for (auto chunkIdx = 0u; chunkIdx < numChunks; chunkIdx++) {
        uint64_t numPages = 0u, offsetInPage = 0u;
        auto lastNodeOffsetInChunk =
            std::min(nodeOffset + ListsMetadataConstants::LISTS_CHUNK_SIZE, numNodes);
        while (nodeOffset < lastNodeOffsetInChunk) {
            auto numElementsInList = listHeaders->getListSize(nodeOffset);
            calculatePagesForList(numPages, offsetInPage, numElementsInList, numElementsPerPage);
            nodeOffset++;
        }
        if (offsetInPage != 0) {
            numPages++;
        }
        listsMetadataBuilder->populateChunkPageList(chunkIdx, numPages, inMemFile->getNumPages());
        inMemFile->addNewPages(numPages);
    }
}

void InMemLists::calculatePagesForList(uint64_t& numPages, uint64_t& offsetInPage,
    uint64_t numElementsInList, uint64_t numElementsPerPage) {
    while (numElementsInList + offsetInPage > numElementsPerPage) {
        numElementsInList -= (numElementsPerPage - offsetInPage);
        numPages++;
        offsetInPage = 0;
    }
    offsetInPage += numElementsInList;
}

void InMemLists::fillInMemListsWithStrValFunc(InMemLists* inMemLists, uint8_t* defaultVal,
    PageByteCursor& pageByteCursor, offset_t nodeOffset, uint64_t posInList,
    const LogicalType& dataType) {
    auto strVal = *(ku_string_t*)defaultVal;
    inMemLists->getInMemOverflowFile()->copyStringOverflow(
        pageByteCursor, reinterpret_cast<uint8_t*>(strVal.overflowPtr), &strVal);
    inMemLists->setValue(nodeOffset, posInList, reinterpret_cast<uint8_t*>(&strVal));
}

void InMemLists::fillInMemListsWithListValFunc(InMemLists* inMemLists, uint8_t* defaultVal,
    PageByteCursor& pageByteCursor, offset_t nodeOffset, uint64_t posInList,
    const LogicalType& dataType) {
    auto listVal = *reinterpret_cast<ku_list_t*>(defaultVal);
    inMemLists->getInMemOverflowFile()->copyListOverflowToFile(
        pageByteCursor, &listVal, VarListType::getChildType(&dataType));
    inMemLists->setValue(nodeOffset, posInList, reinterpret_cast<uint8_t*>(&listVal));
}

fill_in_mem_lists_function_t InMemLists::getFillInMemListsFunc(const LogicalType& dataType) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::INT64:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::BOOL:
    case LogicalTypeID::DATE:
    case LogicalTypeID::TIMESTAMP:
    case LogicalTypeID::INTERVAL:
    case LogicalTypeID::FIXED_LIST: {
        return fillInMemListsWithNonOverflowValFunc;
    }
    case LogicalTypeID::STRING: {
        return fillInMemListsWithStrValFunc;
    }
    case LogicalTypeID::VAR_LIST: {
        return fillInMemListsWithListValFunc;
    }
    default: {
        throw NotImplementedException("InMemLists::getFillInMemListsFunc");
    }
    }
}

template<typename ARROW_TYPE>
void InMemLists::templateCopyArrowStringArray(
    arrow::Array* boundNodeOffsets, arrow::Array* posInRelList, arrow::Array* array) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::DATE: {
        templateCopyArrayAsStringToRelLists<date_t, ARROW_TYPE>(
            boundNodeOffsets, posInRelList, array);
    } break;
        // TODO(Ziyi): Handle timestamp with time zone and units.
    case LogicalTypeID::TIMESTAMP: {
        templateCopyArrayAsStringToRelLists<timestamp_t, ARROW_TYPE>(
            boundNodeOffsets, posInRelList, array);
    } break;
    case LogicalTypeID::INTERVAL: {
        templateCopyArrayAsStringToRelLists<interval_t, ARROW_TYPE>(
            boundNodeOffsets, posInRelList, array);
    } break;
    case LogicalTypeID::FIXED_LIST: {
        // Fixed list is a fixed-sized blob.
        templateCopyArrayAsStringToRelLists<uint8_t*, ARROW_TYPE>(
            boundNodeOffsets, posInRelList, array);
    } break;
        // TODO(Ziyi): Add support of VAR_LIST/STRUCT.
    default: {
        throw CopyException("Unsupported data type ");
    }
    }
}

template<typename KU_TYPE, typename ARROW_TYPE>
void InMemLists::templateCopyArrayAsStringToRelLists(
    arrow::Array* boundNodeOffsets, arrow::Array* posInRelList, arrow::Array* array) {
    auto stringArray = (ARROW_TYPE*)array;
    auto offsets = boundNodeOffsets->data()->GetValues<offset_t>(1);
    auto positions = posInRelList->data()->GetValues<int64_t>(1);
    if (array->data()->MayHaveNulls()) {
        for (auto i = 0u; i < array->length(); i++) {
            if (array->IsNull(i)) {
                continue;
            }
            auto value = stringArray->GetView(i);
            setValueFromString<KU_TYPE>(offsets[i], positions[i], value.data(), value.length());
        }
    } else {
        for (auto i = 0u; i < array->length(); i++) {
            auto value = stringArray->GetView(i);
            setValueFromString<KU_TYPE>(offsets[i], positions[i], value.data(), value.length());
        }
    }
}

void InMemAdjLists::saveToFile() {
    listHeadersBuilder->saveToDisk();
    InMemLists::saveToFile();
}

InMemListsWithOverflow::InMemListsWithOverflow(std::string fName, LogicalType dataType,
    uint64_t numNodes, std::shared_ptr<ListHeadersBuilder> listHeadersBuilder,
    std::unique_ptr<common::CSVReaderConfig> csvReaderConfig)
    : InMemLists{std::move(fName), StorageUtils::getDataTypeSize(dataType), std::move(dataType),
          numNodes, std::move(csvReaderConfig), true},
      blobBuffer{std::make_unique<uint8_t[]>(BufferPoolConstants::PAGE_4KB_SIZE)} {
    assert(this->dataType.getLogicalTypeID() == LogicalTypeID::STRING ||
           this->dataType.getLogicalTypeID() == LogicalTypeID::VAR_LIST);
    overflowInMemFile =
        make_unique<InMemOverflowFile>(StorageUtils::getOverflowFileName(this->fName));
    this->listHeadersBuilder = std::move(listHeadersBuilder);
}

void InMemListsWithOverflow::copyArrowArray(arrow::Array* boundNodeOffsets,
    arrow::Array* posInRelLists, arrow::Array* array, PropertyCopyState* copyState) {
    assert(
        array->type_id() == arrow::Type::STRING || array->type_id() == arrow::Type::LARGE_STRING);
    switch (array->type_id()) {
    case arrow::Type::STRING: {
        templateCopyFromArrowString<arrow::StringArray>(
            boundNodeOffsets, posInRelLists, array, copyState->overflowCursor);
    } break;
    case arrow::Type::LARGE_STRING: {
        templateCopyFromArrowString<arrow::LargeStringArray>(
            boundNodeOffsets, posInRelLists, array, copyState->overflowCursor);
    } break;
    default: {
        throw NotImplementedException{"InMemListsWithOverflow::copyArrowArray"};
    }
    }
}

template<typename T>
void InMemListsWithOverflow::templateCopyFromArrowString(arrow::Array* boundNodeOffsets,
    arrow::Array* posInRelList, arrow::Array* array, PageByteCursor& overflowCursor) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BLOB: {
        templateCopyArrayAsStringToRelListsWithOverflow<blob_t, T>(
            boundNodeOffsets, posInRelList, array, overflowCursor);
    } break;
    case LogicalTypeID::STRING: {
        templateCopyArrayAsStringToRelListsWithOverflow<ku_string_t, T>(
            boundNodeOffsets, posInRelList, array, overflowCursor);
    } break;
    case LogicalTypeID::VAR_LIST: {
        templateCopyArrayAsStringToRelListsWithOverflow<ku_list_t, T>(
            boundNodeOffsets, posInRelList, array, overflowCursor);
    } break;
    default: {
        throw NotImplementedException{"InMemListsWithOverflow::templateCopyFromArrowString"};
    }
    }
}

template<typename KU_TYPE, typename ARROW_TYPE>
void InMemListsWithOverflow::templateCopyArrayAsStringToRelListsWithOverflow(
    arrow::Array* boundNodeOffsets, arrow::Array* posInRelList, arrow::Array* array,
    PageByteCursor& overflowCursor) {
    auto stringArray = (ARROW_TYPE*)array;
    auto offsets = boundNodeOffsets->data()->GetValues<offset_t>(1);
    auto positions = posInRelList->data()->GetValues<int64_t>(1);
    for (auto i = 0u; i < stringArray->length(); i++) {
        auto value = stringArray->GetView(i);
        setValueFromStringWithOverflow<KU_TYPE>(
            overflowCursor, offsets[i], positions[i], value.data(), value.length());
    }
}

template<>
void InMemListsWithOverflow::setValueFromStringWithOverflow<ku_string_t>(
    PageByteCursor& overflowCursor, offset_t nodeOffset, uint64_t pos, const char* val,
    uint64_t length) {
    if (length > BufferPoolConstants::PAGE_4KB_SIZE) {
        throw CopyException(
            ExceptionMessage::overLargeStringValueException(std::to_string(length)));
    }
    auto stringVal = overflowInMemFile->copyString(val, length, overflowCursor);
    setValue(nodeOffset, pos, (uint8_t*)&stringVal);
}

template<>
void InMemListsWithOverflow::setValueFromStringWithOverflow<blob_t>(PageByteCursor& overflowCursor,
    offset_t nodeOffset, uint64_t pos, const char* val, uint64_t length) {
    auto blobLen = Blob::fromString(
        val, std::min(length, BufferPoolConstants::PAGE_4KB_SIZE), blobBuffer.get());
    auto blobVal = overflowInMemFile->copyString(
        reinterpret_cast<const char*>(blobBuffer.get()), blobLen, overflowCursor);
    setValue(nodeOffset, pos, (uint8_t*)&blobVal);
}

template<>
void InMemListsWithOverflow::setValueFromStringWithOverflow<ku_list_t>(
    PageByteCursor& overflowCursor, offset_t nodeOffset, uint64_t pos, const char* val,
    uint64_t length) {
    auto varList = TableCopyUtils::getVarListValue(val, 1, length - 2, dataType, *csvReaderConfig);
    auto listVal = overflowInMemFile->copyList(*varList, overflowCursor);
    setValue(nodeOffset, pos, (uint8_t*)&listVal);
}

void InMemListsWithOverflow::saveToFile() {
    InMemLists::saveToFile();
    overflowInMemFile->flush();
}

std::unique_ptr<InMemLists> InMemListsFactory::getInMemPropertyLists(const std::string& fName,
    const LogicalType& dataType, uint64_t numNodes, common::CSVReaderConfig* csvReaderConfig,
    std::shared_ptr<ListHeadersBuilder> listHeadersBuilder) {
    auto csvReaderConfigCopy = csvReaderConfig ? csvReaderConfig->copy() : nullptr;
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT16:
    case LogicalTypeID::INT8:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::UINT32:
    case LogicalTypeID::UINT16:
    case LogicalTypeID::UINT8:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::BOOL:
    case LogicalTypeID::DATE:
    case LogicalTypeID::TIMESTAMP:
    case LogicalTypeID::INTERVAL:
    case LogicalTypeID::FIXED_LIST:
        return make_unique<InMemLists>(fName, dataType,
            storage::StorageUtils::getDataTypeSize(dataType), numNodes,
            std::move(listHeadersBuilder), std::move(csvReaderConfigCopy), true /* hasNULLBytes */);
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRING:
        return make_unique<InMemStringLists>(fName, numNodes, std::move(listHeadersBuilder));
    case LogicalTypeID::VAR_LIST:
        return make_unique<InMemListLists>(fName, dataType, numNodes, std::move(listHeadersBuilder),
            std::move(csvReaderConfigCopy));
    case LogicalTypeID::INTERNAL_ID:
        return make_unique<InMemRelIDLists>(fName, numNodes, std::move(listHeadersBuilder));
    default:
        throw CopyException("Invalid type for property list creation.");
    }
}

} // namespace storage
} // namespace kuzu
