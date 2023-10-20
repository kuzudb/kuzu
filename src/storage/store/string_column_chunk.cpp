#include "storage/store/string_column_chunk.h"

#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/exception/not_implemented.h"
#include "storage/store/table_copy_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

StringColumnChunk::StringColumnChunk(LogicalType dataType) : ColumnChunk{std::move(dataType)} {
    overflowFile = std::make_unique<InMemOverflowFile>();
    overflowCursor.pageIdx = 0;
    overflowCursor.offsetInPage = 0;
}

void StringColumnChunk::resetToEmpty() {
    ColumnChunk::resetToEmpty();
    overflowFile = std::make_unique<InMemOverflowFile>();
    overflowCursor.resetValue();
}

void StringColumnChunk::append(ValueVector* vector, offset_t startPosInChunk) {
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
            setValueFromString(kuStr.getAsString().c_str(), kuStr.len, offsetInChunk);
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
    setValueFromString(strVal.c_str(), strVal.length(), posToWrite);
}

void StringColumnChunk::setValueFromString(const char* value, uint64_t length, uint64_t pos) {
    TableCopyUtils::validateStrLen(length);
    auto val = overflowFile->copyString(value, length, overflowCursor);
    setValue(val, pos);
}

// STRING
template<>
std::string StringColumnChunk::getValue<std::string>(offset_t pos) const {
    auto kuStr = ((ku_string_t*)buffer.get())[pos];
    return overflowFile->readString(&kuStr);
}

} // namespace storage
} // namespace kuzu
