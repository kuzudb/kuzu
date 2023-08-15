#include "storage/copier/string_column_chunk.h"

#include "storage/copier/table_copy_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

StringColumnChunk::StringColumnChunk(LogicalType dataType, CopyDescription* copyDescription)
    : ColumnChunk{std::move(dataType), copyDescription} {
    overflowFile = std::make_unique<InMemOverflowFile>();
    overflowCursor.pageIdx = 0;
    overflowCursor.offsetInPage = 0;
}

void StringColumnChunk::resetToEmpty() {
    ColumnChunk::resetToEmpty();
    overflowFile = std::make_unique<InMemOverflowFile>();
    overflowCursor.resetValue();
}

void StringColumnChunk::append(common::ValueVector* vector, common::offset_t startPosInChunk) {
    assert(vector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    ColumnChunk::copyVectorToBuffer(vector, startPosInChunk);
    auto stringsToSetOverflow = (ku_string_t*)(buffer.get() + startPosInChunk * numBytesPerValue);
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        auto& stringToSet = stringsToSetOverflow[i];
        if (!ku_string_t::isShortString(stringToSet.len)) {
            overflowFile->copyStringOverflow(
                overflowCursor, reinterpret_cast<uint8_t*>(stringToSet.overflowPtr), &stringToSet);
        }
    }
    numValues += vector->state->selVector->selectedSize;
}

void StringColumnChunk::append(
    arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    assert(
        array->type_id() == arrow::Type::STRING || array->type_id() == arrow::Type::LARGE_STRING);
    switch (array->type_id()) {
    case arrow::Type::STRING: {
        templateCopyStringArrowArray<arrow::StringArray>(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::LARGE_STRING: {
        templateCopyStringArrowArray<arrow::LargeStringArray>(
            array, startPosInChunk, numValuesToAppend);
    } break;
    default: {
        throw NotImplementedException("StringColumnChunk::append");
    }
    }
    numValues += numValuesToAppend;
}

void StringColumnChunk::append(ColumnChunk* other, offset_t startPosInOtherChunk,
    offset_t startPosInChunk, uint32_t numValuesToAppend) {
    auto otherChunk = reinterpret_cast<StringColumnChunk*>(other);
    nullChunk->append(
        otherChunk->getNullChunk(), startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRING: {
        appendStringColumnChunk(
            otherChunk, startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    } break;
    default: {
        throw NotImplementedException("VarSizedColumnChunk::append");
    }
    }
    numValues += numValuesToAppend;
}

void StringColumnChunk::update(ValueVector* vector, vector_idx_t vectorIdx) {
    auto startOffsetInChunk = vectorIdx << DEFAULT_VECTOR_CAPACITY_LOG_2;
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        auto pos = vector->state->selVector->selectedPositions[i];
        auto offsetInChunk = startOffsetInChunk + pos;
        nullChunk->setNull(offsetInChunk, vector->isNull(pos));
        if (!vector->isNull(pos)) {
            auto kuStr = vector->getValue<ku_string_t>(pos);
            setValueFromString<ku_string_t>(kuStr.getAsString().c_str(), kuStr.len, offsetInChunk);
        }
    }
}
page_idx_t StringColumnChunk::flushOverflowBuffer(BMFileHandle* dataFH, page_idx_t startPageIdx) {
    for (auto i = 0u; i < overflowFile->getNumPages(); i++) {
        FileUtils::writeToFile(dataFH->getFileInfo(), overflowFile->getPage(i)->data,
            BufferPoolConstants::PAGE_4KB_SIZE, startPageIdx * BufferPoolConstants::PAGE_4KB_SIZE);
        startPageIdx++;
    }
    return overflowFile->getNumPages();
}

void StringColumnChunk::appendStringColumnChunk(StringColumnChunk* other,
    offset_t startPosInOtherChunk, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    auto otherKuVals = reinterpret_cast<ku_string_t*>(other->buffer.get());
    auto kuVals = reinterpret_cast<ku_string_t*>(buffer.get());
    for (auto i = 0u; i < numValuesToAppend; i++) {
        auto posInChunk = i + startPosInChunk;
        auto posInOtherChunk = i + startPosInOtherChunk;
        kuVals[posInChunk] = otherKuVals[posInOtherChunk];
        if (other->nullChunk->isNull(posInOtherChunk) ||
            otherKuVals[posInOtherChunk].len <= ku_string_t::SHORT_STR_LENGTH) {
            continue;
        }
        PageByteCursor cursorToCopyFrom;
        TypeUtils::decodeOverflowPtr(otherKuVals[posInOtherChunk].overflowPtr,
            cursorToCopyFrom.pageIdx, cursorToCopyFrom.offsetInPage);
        overflowFile->copyStringOverflow(overflowCursor,
            other->overflowFile->getPage(cursorToCopyFrom.pageIdx)->data +
                cursorToCopyFrom.offsetInPage,
            &kuVals[posInChunk]);
    }
}

void StringColumnChunk::write(const Value& val, uint64_t posToWrite) {
    assert(val.getDataType()->getPhysicalType() == PhysicalTypeID::STRING);
    nullChunk->setNull(posToWrite, val.isNull());
    if (val.isNull()) {
        return;
    }
    auto strVal = val.getValue<std::string>();
    setValueFromString<ku_string_t>(strVal.c_str(), strVal.length(), posToWrite);
}

// BLOB
template<>
void StringColumnChunk::setValueFromString<blob_t>(
    const char* value, uint64_t length, uint64_t pos) {
    if (length > BufferPoolConstants::PAGE_4KB_SIZE) {
        throw CopyException(
            ExceptionMessage::overLargeStringValueException(std::to_string(length)));
    }
    auto blobBuffer = std::make_unique<uint8_t[]>(length);
    auto blobLen = Blob::fromString(value, length, blobBuffer.get());
    auto val = overflowFile->copyString((char*)blobBuffer.get(), blobLen, overflowCursor);
    setValue(val, pos);
}

// STRING
template<>
void StringColumnChunk::setValueFromString<ku_string_t>(
    const char* value, uint64_t length, uint64_t pos) {
    if (length > BufferPoolConstants::PAGE_4KB_SIZE) {
        throw CopyException(
            ExceptionMessage::overLargeStringValueException(std::to_string(length)));
    }
    auto val = overflowFile->copyString(value, length, overflowCursor);
    setValue(val, pos);
}

// STRING
template<>
std::string StringColumnChunk::getValue<std::string>(offset_t pos) const {
    auto kuStr = ((ku_string_t*)buffer.get())[pos];
    return overflowFile->readString(&kuStr);
}

template<typename T>
void StringColumnChunk::templateCopyStringArrowArray(
    arrow::Array* array, common::offset_t startPosInChunk, uint32_t numValuesToAppend) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BLOB: {
        templateCopyStringValues<blob_t, T>(array, startPosInChunk, numValuesToAppend);
    } break;
    case LogicalTypeID::STRING: {
        templateCopyStringValues<ku_string_t, T>(array, startPosInChunk, numValuesToAppend);
    } break;
    default: {
        throw NotImplementedException("StringColumnChunk::templateCopyStringArrowArray");
    }
    }
}

template<typename KU_TYPE, typename ARROW_TYPE>
void StringColumnChunk::templateCopyStringValues(
    arrow::Array* array, common::offset_t startPosInChunk, uint32_t numValuesToAppend) {
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

} // namespace storage
} // namespace kuzu
