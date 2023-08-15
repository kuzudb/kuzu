#include "storage/copier/column_chunk.h"

#include "arrow/array.h"
#include "storage/copier/string_column_chunk.h"
#include "storage/copier/struct_column_chunk.h"
#include "storage/copier/table_copy_utils.h"
#include "storage/copier/var_list_column_chunk.h"
#include "storage/storage_structure/storage_structure_utils.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

ColumnChunk::ColumnChunk(LogicalType dataType, CopyDescription* copyDescription, bool hasNullChunk)
    : dataType{std::move(dataType)}, numBytesPerValue{getDataTypeSizeInChunk(this->dataType)},
      copyDescription{copyDescription}, numValues{0} {
    if (hasNullChunk) {
        nullChunk = std::make_unique<NullColumnChunk>();
    }
}

void ColumnChunk::initialize(offset_t capacity) {
    bufferSize = numBytesPerValue * capacity;
    buffer = std::make_unique<uint8_t[]>(bufferSize);
    if (nullChunk) {
        static_cast<ColumnChunk*>(nullChunk.get())->initialize(capacity);
    }
}

void ColumnChunk::resetToEmpty() {
    if (nullChunk) {
        nullChunk->resetNullBuffer();
    }
    numValues = 0;
}

void ColumnChunk::append(common::ValueVector* vector, common::offset_t startPosInChunk) {
    switch (vector->dataType.getPhysicalType()) {
    case PhysicalTypeID::INT64:
    case PhysicalTypeID::INT32:
    case PhysicalTypeID::INT16:
    case PhysicalTypeID::DOUBLE:
    case PhysicalTypeID::FLOAT:
    case PhysicalTypeID::INTERVAL:
    case PhysicalTypeID::FIXED_LIST: {
        copyVectorToBuffer(vector, startPosInChunk);
    } break;
    default: {
        throw NotImplementedException{"ColumnChunk::append"};
    }
    }
    numValues += vector->state->selVector->selectedSize;
}

void ColumnChunk::append(
    ValueVector* vector, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    assert(vector->dataType.getLogicalTypeID() == LogicalTypeID::ARROW_COLUMN);
    auto chunkedArray = ArrowColumnVector::getArrowColumn(vector).get();
    for (auto array : chunkedArray->chunks()) {
        auto numValuesInArrayToAppend =
            std::min((uint64_t)array->length(), (uint64_t)numValuesToAppend);
        if (numValuesInArrayToAppend <= 0) {
            break;
        }
        append(array.get(), startPosInChunk, numValuesInArrayToAppend);
        numValuesToAppend -= numValuesInArrayToAppend;
        startPosInChunk += numValuesInArrayToAppend;
    }
}

void ColumnChunk::append(ColumnChunk* other, offset_t startPosInOtherChunk,
    offset_t startPosInChunk, uint32_t numValuesToAppend) {
    if (nullChunk) {
        nullChunk->append(
            other->nullChunk.get(), startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    }
    memcpy(buffer.get() + startPosInChunk * numBytesPerValue,
        other->buffer.get() + startPosInOtherChunk * numBytesPerValue,
        numValuesToAppend * numBytesPerValue);
    numValues += numValuesToAppend;
}

void ColumnChunk::append(
    arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    switch (array->type_id()) {
    case arrow::Type::INT16: {
        templateCopyArrowArray<int16_t>(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::INT32: {
        templateCopyArrowArray<int32_t>(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::INT64: {
        templateCopyArrowArray<int64_t>(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::DOUBLE: {
        templateCopyArrowArray<double_t>(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::FLOAT: {
        templateCopyArrowArray<float_t>(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::DATE32: {
        templateCopyArrowArray<date_t>(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::TIMESTAMP: {
        templateCopyArrowArray<timestamp_t>(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::FIXED_SIZE_LIST: {
        templateCopyArrowArray<uint8_t*>(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::STRING: {
        templateCopyStringArrowArray<arrow::StringArray>(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::LARGE_STRING: {
        templateCopyStringArrowArray<arrow::LargeStringArray>(
            array, startPosInChunk, numValuesToAppend);
    } break;
    default: {
        throw NotImplementedException("ColumnChunk::append");
    }
    }
    numValues += numValuesToAppend;
}

void ColumnChunk::write(const Value& val, uint64_t posToWrite) {
    nullChunk->setNull(posToWrite, val.isNull());
    if (val.isNull()) {
        return;
    }
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        setValue(val.getValue<bool>(), posToWrite);
    } break;
    case PhysicalTypeID::INT64: {
        setValue(val.getValue<int64_t>(), posToWrite);
    } break;
    case PhysicalTypeID::INT32: {
        setValue(val.getValue<int32_t>(), posToWrite);
    } break;
    case PhysicalTypeID::INT16: {
        setValue(val.getValue<int16_t>(), posToWrite);
    } break;
    case PhysicalTypeID::DOUBLE: {
        setValue(val.getValue<double_t>(), posToWrite);
    } break;
    case PhysicalTypeID::FLOAT: {
        setValue(val.getValue<float_t>(), posToWrite);
    } break;
    case PhysicalTypeID::INTERVAL: {
        setValue(val.getValue<interval_t>(), posToWrite);
    } break;
    case PhysicalTypeID::INTERNAL_ID: {
        setValue(val.getValue<internalID_t>(), posToWrite);
    } break;
    default: {
        throw NotImplementedException{"ColumnChunk::write"};
    }
    }
}

void ColumnChunk::resize(uint64_t numValues) {
    auto numBytesAfterResize = numValues * numBytesPerValue;
    auto resizedBuffer = std::make_unique<uint8_t[]>(numBytesAfterResize);
    memcpy(resizedBuffer.get(), buffer.get(), bufferSize);
    bufferSize = numBytesAfterResize;
    buffer = std::move(resizedBuffer);
    if (nullChunk) {
        nullChunk->resize(numValues);
    }
    for (auto& child : childrenChunks) {
        child->resize(numValues);
    }
}

void ColumnChunk::populateWithDefaultVal(common::ValueVector* defaultValueVector) {
    defaultValueVector->setState(std::make_shared<common::DataChunkState>());
    auto valPos = defaultValueVector->state->selVector->selectedPositions[0];
    defaultValueVector->state->selVector->resetSelectorToValuePosBufferWithSize(
        common::DEFAULT_VECTOR_CAPACITY);
    for (auto i = 0u; i < defaultValueVector->state->selVector->selectedSize; i++) {
        defaultValueVector->state->selVector->selectedPositions[i] = valPos;
    }
    auto numValuesAppended = 0u;
    while (numValuesAppended < common::StorageConstants::NODE_GROUP_SIZE) {
        auto numValuesToAppend = std::min(common::DEFAULT_VECTOR_CAPACITY,
            common::StorageConstants::NODE_GROUP_SIZE - numValuesAppended);
        defaultValueVector->state->selVector->selectedSize = numValuesToAppend;
        append(defaultValueVector, numValuesAppended);
        numValuesAppended += numValuesToAppend;
    }
}

template<typename T>
void ColumnChunk::templateCopyArrowArray(
    arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    const auto& arrowArray = array->data();
    auto valuesInChunk = (T*)buffer.get();
    auto valuesInArray = arrowArray->GetValues<T>(1 /* value buffer */);
    if (arrowArray->MayHaveNulls()) {
        for (auto i = 0u; i < numValuesToAppend; i++) {
            auto posInChunk = startPosInChunk + i;
            if (arrowArray->IsNull(i)) {
                nullChunk->setNull(posInChunk, true);
                continue;
            }
            valuesInChunk[posInChunk] = valuesInArray[i];
        }
    } else {
        for (auto i = 0u; i < numValuesToAppend; i++) {
            auto posInChunk = startPosInChunk + i;
            valuesInChunk[posInChunk] = valuesInArray[i];
        }
    }
}

template<typename ARROW_TYPE>
void ColumnChunk::templateCopyStringArrowArray(
    arrow::Array* array, common::offset_t startPosInChunk, uint32_t numValuesToAppend) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::DATE: {
        templateCopyValuesAsString<date_t, ARROW_TYPE>(array, startPosInChunk, numValuesToAppend);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        templateCopyValuesAsString<timestamp_t, ARROW_TYPE>(
            array, startPosInChunk, numValuesToAppend);
    } break;
    case LogicalTypeID::INTERVAL: {
        templateCopyValuesAsString<interval_t, ARROW_TYPE>(
            array, startPosInChunk, numValuesToAppend);
    } break;
    case LogicalTypeID::FIXED_LIST: {
        // Fixed list is a fixed-sized blob.
        templateCopyValuesAsString<uint8_t*, ARROW_TYPE>(array, startPosInChunk, numValuesToAppend);
    } break;
    default: {
        throw NotImplementedException("ColumnChunk::templateCopyStringArrowArray");
    }
    }
}

template<>
void ColumnChunk::templateCopyArrowArray<bool>(
    arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    auto* boolArray = (arrow::BooleanArray*)array;
    auto data = boolArray->data();

    auto arrowBuffer = boolArray->values()->data();
    // Might read off the end with the cast, but copyNullMask should ignore the extra data
    //
    // The arrow BooleanArray offset should be the offset in bits
    // Unfortunately this is not documented.
    NullMask::copyNullMask((uint64_t*)arrowBuffer, boolArray->offset(), (uint64_t*)buffer.get(),
        startPosInChunk, numValuesToAppend);

    if (data->MayHaveNulls()) {
        auto arrowNullBitMap = boolArray->null_bitmap_data();

        // Offset should apply to both bool data and nulls
        NullMask::copyNullMask((uint64_t*)arrowNullBitMap, boolArray->offset(),
            (uint64_t*)nullChunk->buffer.get(), startPosInChunk, numValuesToAppend,
            true /*invert*/);
    }
}

template<>
void ColumnChunk::templateCopyArrowArray<uint8_t*>(
    arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    auto fixedSizedListArray = (arrow::FixedSizeListArray*)array;
    auto valuesInList = (uint8_t*)fixedSizedListArray->values()->data()->buffers[1]->data();
    if (fixedSizedListArray->data()->MayHaveNulls()) {
        for (auto i = 0u; i < numValuesToAppend; i++) {
            auto posInChunk = startPosInChunk + i;
            if (fixedSizedListArray->data()->IsNull(i)) {
                nullChunk->setNull(posInChunk, true);
                continue;
            }
            auto posInList = fixedSizedListArray->offset() + i;
            memcpy(buffer.get() + getOffsetInBuffer(posInChunk),
                valuesInList + posInList * numBytesPerValue, numBytesPerValue);
        }
    } else {
        for (auto i = 0u; i < numValuesToAppend; i++) {
            auto posInChunk = startPosInChunk + i;
            auto posInList = fixedSizedListArray->offset() + i;
            memcpy(buffer.get() + getOffsetInBuffer(posInChunk),
                valuesInList + posInList * numBytesPerValue, numBytesPerValue);
        }
    }
}

template<typename KU_TYPE, typename ARROW_TYPE>
void ColumnChunk::templateCopyValuesAsString(
    arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    auto stringArray = (ARROW_TYPE*)array;
    auto arrayData = stringArray->data();
    if (arrayData->MayHaveNulls()) {
        for (auto i = 0u; i < numValuesToAppend; i++) {
            auto posInChunk = startPosInChunk + i;
            if (arrayData->IsNull(i)) {
                nullChunk->setNull(posInChunk, true);
                continue;
            }
            auto value = stringArray->GetView(i);
            setValueFromString<KU_TYPE>(value.data(), value.length(), posInChunk);
        }
    } else {
        for (auto i = 0u; i < numValuesToAppend; i++) {
            auto posInChunk = startPosInChunk + i;
            auto value = stringArray->GetView(i);
            setValueFromString<KU_TYPE>(value.data(), value.length(), posInChunk);
        }
    }
}

page_idx_t ColumnChunk::getNumPages() const {
    auto numPagesToFlush = getNumPagesForBuffer();
    if (nullChunk) {
        numPagesToFlush += nullChunk->getNumPages();
    }
    for (auto& child : childrenChunks) {
        numPagesToFlush += child->getNumPages();
    }
    return numPagesToFlush;
}

page_idx_t ColumnChunk::flushBuffer(BMFileHandle* dataFH, page_idx_t startPageIdx) {
    FileUtils::writeToFile(dataFH->getFileInfo(), buffer.get(), bufferSize,
        startPageIdx * BufferPoolConstants::PAGE_4KB_SIZE);
    return getNumPagesForBuffer();
}

uint32_t ColumnChunk::getDataTypeSizeInChunk(LogicalType& dataType) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::STRUCT: {
        return 0;
    }
    case LogicalTypeID::STRING: {
        return sizeof(ku_string_t);
    }
    case LogicalTypeID::VAR_LIST: {
        return sizeof(offset_t);
    }
    case LogicalTypeID::INTERNAL_ID: {
        return sizeof(offset_t);
    }
    case LogicalTypeID::SERIAL: {
        return sizeof(int64_t);
    }
    // Used by NodeColumnn to represent the de-compressed size of booleans in-memory
    // Does not reflect the size of booleans on-disk in BoolColumnChunk
    case LogicalTypeID::BOOL: {
        return 1;
    }
    default: {
        return StorageUtils::getDataTypeSize(dataType);
    }
    }
}

void BoolColumnChunk::append(common::ValueVector* vector, common::offset_t startPosInChunk) {
    assert(vector->dataType.getPhysicalType() == PhysicalTypeID::BOOL);
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        auto pos = vector->state->selVector->selectedPositions[i];
        nullChunk->setNull(startPosInChunk + i, vector->isNull(pos));
        common::NullMask::setNull(
            (uint64_t*)buffer.get(), startPosInChunk + i, vector->getValue<bool>(pos));
    }
    numValues += vector->state->selVector->selectedSize;
}

void BoolColumnChunk::append(
    arrow::Array* array, common::offset_t startPosInChunk, uint32_t numValuesToAppend) {
    assert(array->type_id() == arrow::Type::BOOL);
    templateCopyArrowArray<bool>(array, startPosInChunk, numValuesToAppend);
    numValues += numValuesToAppend;
}

void BoolColumnChunk::append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
    common::offset_t startPosInChunk, uint32_t numValuesToAppend) {
    NullMask::copyNullMask((uint64_t*)static_cast<BoolColumnChunk*>(other)->buffer.get(),
        startPosInOtherChunk, (uint64_t*)buffer.get(), startPosInChunk, numValuesToAppend);

    if (nullChunk) {
        nullChunk->append(
            other->getNullChunk(), startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    }
    numValues += numValuesToAppend;
}

void FixedListColumnChunk::append(ColumnChunk* other, offset_t startPosInOtherChunk,
    common::offset_t startPosInChunk, uint32_t numValuesToAppend) {
    auto otherChunk = (FixedListColumnChunk*)other;
    if (nullChunk) {
        nullChunk->append(
            otherChunk->nullChunk.get(), startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    }
    // TODO(Guodong): This can be optimized to not copy one by one.
    for (auto i = 0u; i < numValuesToAppend; i++) {
        memcpy(buffer.get() + getOffsetInBuffer(startPosInChunk + i),
            otherChunk->buffer.get() + getOffsetInBuffer(startPosInOtherChunk + i),
            numBytesPerValue);
    }
    numValues += numValuesToAppend;
}

void FixedListColumnChunk::write(const Value& fixedListVal, uint64_t posToWrite) {
    assert(fixedListVal.getDataType()->getPhysicalType() == PhysicalTypeID::FIXED_LIST);
    nullChunk->setNull(posToWrite, fixedListVal.isNull());
    if (fixedListVal.isNull()) {
        return;
    }
    auto numValues = NestedVal::getChildrenSize(&fixedListVal);
    auto childType = FixedListType::getChildType(fixedListVal.getDataType());
    auto numBytesPerValueInList = getDataTypeSizeInChunk(*childType);
    auto bufferToWrite = buffer.get() + posToWrite * numBytesPerValue;
    for (auto i = 0u; i < numValues; i++) {
        auto val = NestedVal::getChildVal(&fixedListVal, i);
        switch (childType->getPhysicalType()) {
        case PhysicalTypeID::INT64: {
            memcpy(bufferToWrite, &val->getValueReference<int64_t>(), numBytesPerValueInList);
        } break;
        case PhysicalTypeID::INT32: {
            memcpy(bufferToWrite, &val->getValueReference<int32_t>(), numBytesPerValueInList);
        } break;
        case PhysicalTypeID::INT16: {
            memcpy(bufferToWrite, &val->getValueReference<int16_t>(), numBytesPerValueInList);
        } break;
        case PhysicalTypeID::DOUBLE: {
            memcpy(bufferToWrite, &val->getValueReference<double_t>(), numBytesPerValueInList);
        } break;
        case PhysicalTypeID::FLOAT: {
            memcpy(bufferToWrite, &val->getValueReference<float_t>(), numBytesPerValueInList);
        } break;
        default: {
            throw NotImplementedException{"FixedListColumnChunk::write"};
        }
        }
        bufferToWrite += numBytesPerValueInList;
    }
}

std::unique_ptr<ColumnChunk> ColumnChunkFactory::createColumnChunk(
    const LogicalType& dataType, CopyDescription* copyDescription) {
    std::unique_ptr<ColumnChunk> chunk;
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        chunk = std::make_unique<BoolColumnChunk>(copyDescription);
    } break;
    case PhysicalTypeID::INT64:
    case PhysicalTypeID::INT32:
    case PhysicalTypeID::INT16:
    case PhysicalTypeID::DOUBLE:
    case PhysicalTypeID::FLOAT:
    case PhysicalTypeID::INTERVAL: {
        if (dataType.getLogicalTypeID() == LogicalTypeID::SERIAL) {
            chunk = std::make_unique<SerialColumnChunk>();
        } else {
            chunk = std::make_unique<ColumnChunk>(dataType, copyDescription);
        }
    } break;
    case PhysicalTypeID::FIXED_LIST: {
        chunk = std::make_unique<FixedListColumnChunk>(dataType, copyDescription);
    } break;
    case PhysicalTypeID::STRING: {
        chunk = std::make_unique<StringColumnChunk>(dataType, copyDescription);
    } break;
    case PhysicalTypeID::VAR_LIST: {
        chunk = std::make_unique<VarListColumnChunk>(dataType, copyDescription);
    } break;
    case PhysicalTypeID::STRUCT: {
        chunk = std::make_unique<StructColumnChunk>(dataType, copyDescription);
    } break;
    default: {
        throw NotImplementedException("ColumnChunkFactory::createColumnChunk for data type " +
                                      LogicalTypeUtils::dataTypeToString(dataType) +
                                      " is not supported.");
    }
    }
    chunk->initialize(StorageConstants::NODE_GROUP_SIZE);
    return chunk;
}

// Bool
template<>
void ColumnChunk::setValueFromString<bool>(const char* value, uint64_t length, uint64_t pos) {
    std::istringstream boolStream{std::string(value)};
    bool booleanVal;
    boolStream >> std::boolalpha >> booleanVal;
    setValue(booleanVal, pos);
}

// Fixed list
template<>
void ColumnChunk::setValueFromString<uint8_t*>(const char* value, uint64_t length, uint64_t pos) {
    auto fixedListVal =
        TableCopyUtils::getArrowFixedList(value, 1, length - 2, dataType, *copyDescription);
    memcpy(buffer.get() + pos * numBytesPerValue, fixedListVal.get(), numBytesPerValue);
}

// Interval
template<>
void ColumnChunk::setValueFromString<interval_t>(const char* value, uint64_t length, uint64_t pos) {
    auto val = Interval::fromCString(value, length);
    setValue(val, pos);
}

// Date
template<>
void ColumnChunk::setValueFromString<date_t>(const char* value, uint64_t length, uint64_t pos) {
    auto val = Date::fromCString(value, length);
    setValue(val, pos);
}

// Timestamp
template<>
void ColumnChunk::setValueFromString<timestamp_t>(
    const char* value, uint64_t length, uint64_t pos) {
    auto val = Timestamp::fromCString(value, length);
    setValue(val, pos);
}

offset_t ColumnChunk::getOffsetInBuffer(offset_t pos) const {
    auto numElementsInAPage =
        PageUtils::getNumElementsInAPage(numBytesPerValue, false /* hasNull */);
    auto posCursor = PageUtils::getPageByteCursorForPos(pos, numElementsInAPage, numBytesPerValue);
    auto offsetInBuffer =
        posCursor.pageIdx * BufferPoolConstants::PAGE_4KB_SIZE + posCursor.offsetInPage;
    assert(offsetInBuffer + numBytesPerValue <= bufferSize);
    return offsetInBuffer;
}

void ColumnChunk::copyVectorToBuffer(
    common::ValueVector* vector, common::offset_t startPosInChunk) {
    auto bufferToWrite = buffer.get() + startPosInChunk * numBytesPerValue;
    auto vectorDataToWriteFrom = vector->getData();
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        auto pos = vector->state->selVector->selectedPositions[i];
        nullChunk->setNull(startPosInChunk + i, vector->isNull(pos));
        memcpy(bufferToWrite, vectorDataToWriteFrom + pos * numBytesPerValue, numBytesPerValue);
        bufferToWrite += numBytesPerValue;
    }
}

void NullColumnChunk::resize(uint64_t numValues) {
    auto numBytesAfterResize = numBytesForValues(numValues);
    assert(numBytesAfterResize > bufferSize);
    auto reservedBuffer = std::make_unique<uint8_t[]>(numBytesAfterResize);
    memset(reservedBuffer.get(), 0 /* non null */, numBytesAfterResize);
    memcpy(reservedBuffer.get(), buffer.get(), bufferSize);
    buffer = std::move(reservedBuffer);
    bufferSize = numBytesAfterResize;
}

} // namespace storage
} // namespace kuzu
