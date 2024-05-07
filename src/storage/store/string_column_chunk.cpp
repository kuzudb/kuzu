#include "storage/store/string_column_chunk.h"

#include "common/data_chunk/sel_vector.h"
#include "storage/store/column_chunk.h"
#include "storage/store/dictionary_chunk.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

StringColumnChunk::StringColumnChunk(LogicalType dataType, uint64_t capacity,
    bool enableCompression, bool inMemory)
    : ColumnChunk{std::move(dataType), capacity, enableCompression},
      dictionaryChunk{
          std::make_unique<DictionaryChunk>(inMemory ? 0 : capacity, enableCompression)},
      needFinalize{false} {}

void StringColumnChunk::resetToEmpty() {
    ColumnChunk::resetToEmpty();
    dictionaryChunk->resetToEmpty();
}

void StringColumnChunk::append(ValueVector* vector, const SelectionVector& selVector) {
    for (auto i = 0u; i < selVector.getSelSize(); i++) {
        // index is stored in main chunk, data is stored in the data chunk
        auto pos = selVector[i];
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
        // index is stored in main chunk, data is stored in the data chunk
        nullChunk->setNull(numValues, vector->isNull(pos));
        auto dstPos = numValues++;
        if (vector->isNull(pos)) {
            continue;
        }
        auto kuString = vector->getValue<ku_string_t>(pos);
        setValueFromString(kuString.getAsStringView(), dstPos);
    }
}

void StringColumnChunk::append(ColumnChunk* other, offset_t startPosInOtherChunk,
    uint32_t numValuesToAppend) {
    auto otherChunk = ku_dynamic_cast<ColumnChunk*, StringColumnChunk*>(other);
    nullChunk->append(otherChunk->getNullChunk(), startPosInOtherChunk, numValuesToAppend);
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRING: {
        appendStringColumnChunk(otherChunk, startPosInOtherChunk, numValuesToAppend);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void StringColumnChunk::lookup(offset_t offsetInChunk, ValueVector& output,
    sel_t posInOutputVector) const {
    KU_ASSERT(offsetInChunk < numValues);
    output.setNull(posInOutputVector, nullChunk->isNull(offsetInChunk));
    if (nullChunk->isNull(offsetInChunk)) {
        return;
    }
    auto str = getValue<std::string_view>(offsetInChunk);
    output.setValue<std::string_view>(posInOutputVector, str);
}

void StringColumnChunk::write(ValueVector* vector, offset_t offsetInVector,
    offset_t offsetInChunk) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    if (!needFinalize && offsetInChunk < numValues) [[unlikely]] {
        needFinalize = true;
    }
    nullChunk->setNull(offsetInChunk, vector->isNull(offsetInVector));
    if (offsetInChunk >= numValues) {
        numValues = offsetInChunk + 1;
    }
    if (!vector->isNull(offsetInVector)) {
        auto kuStr = vector->getValue<ku_string_t>(offsetInVector);
        setValueFromString(kuStr.getAsStringView(), offsetInChunk);
    }
}

void StringColumnChunk::write(ColumnChunk* chunk, ColumnChunk* dstOffsets,
    RelMultiplicity /*multiplicity*/) {
    KU_ASSERT(chunk->getDataType().getPhysicalType() == PhysicalTypeID::STRING &&
              dstOffsets->getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID &&
              chunk->getNumValues() == dstOffsets->getNumValues());
    auto stringChunk = ku_dynamic_cast<ColumnChunk*, StringColumnChunk*>(chunk);
    for (auto i = 0u; i < chunk->getNumValues(); i++) {
        auto offsetInChunk = dstOffsets->getValue<offset_t>(i);
        if (!needFinalize && offsetInChunk < numValues) [[unlikely]] {
            needFinalize = true;
        }
        bool isNull = chunk->getNullChunk()->isNull(i);
        nullChunk->setNull(offsetInChunk, isNull);
        if (offsetInChunk >= numValues) {
            numValues = offsetInChunk + 1;
        }
        if (!isNull) {
            setValueFromString(stringChunk->getValue<std::string_view>(i), offsetInChunk);
        }
    }
}

void StringColumnChunk::write(ColumnChunk* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->getDataType().getPhysicalType() == PhysicalTypeID::STRING);
    if ((dstOffsetInChunk + numValuesToCopy) >= numValues) {
        numValues = dstOffsetInChunk + numValuesToCopy;
    }
    auto srcStringChunk = ku_dynamic_cast<ColumnChunk*, StringColumnChunk*>(srcChunk);
    for (auto i = 0u; i < numValuesToCopy; i++) {
        auto srcPos = srcOffsetInChunk + i;
        auto dstPos = dstOffsetInChunk + i;
        bool isNull = srcChunk->getNullChunk()->isNull(srcPos);
        nullChunk->setNull(dstPos, isNull);
        if (isNull) {
            continue;
        }
        setValueFromString(srcStringChunk->getValue<std::string_view>(srcPos), dstPos);
    }
}

void StringColumnChunk::copy(ColumnChunk* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->getDataType().getPhysicalType() == PhysicalTypeID::STRING);
    KU_ASSERT(dstOffsetInChunk >= numValues);
    auto indices = reinterpret_cast<DictionaryChunk::string_index_t*>(buffer.get());
    while (numValues < dstOffsetInChunk) {
        indices[numValues] = 0;
        nullChunk->setNull(numValues, true);
        numValues++;
    }
    auto srcStringChunk = ku_dynamic_cast<ColumnChunk*, StringColumnChunk*>(srcChunk);
    append(srcStringChunk, srcOffsetInChunk, numValuesToCopy);
}

void StringColumnChunk::appendStringColumnChunk(StringColumnChunk* other,
    offset_t startPosInOtherChunk, uint32_t numValuesToAppend) {
    auto indices = reinterpret_cast<DictionaryChunk::string_index_t*>(buffer.get());
    for (auto i = 0u; i < numValuesToAppend; i++) {
        auto posInChunk = numValues;
        auto posInOtherChunk = i + startPosInOtherChunk;
        numValues++;
        if (nullChunk->isNull(posInChunk)) {
            indices[posInChunk] = 0;
            continue;
        }
        setValueFromString(other->getValue<std::string_view>(posInOtherChunk), posInChunk);
    }
}

void StringColumnChunk::setValueFromString(std::string_view value, uint64_t pos) {
    auto index = dictionaryChunk->appendString(value);
    ColumnChunk::setValue<DictionaryChunk::string_index_t>(index, pos);
}

void StringColumnChunk::finalize() {
    if (!needFinalize) {
        return;
    }
    // Prune unused entries in the dictionary before we flush
    // We already de-duplicate as we go, but when out of place updates occur new values will be
    // appended to the end and the original values may be able to be pruned before flushing them to
    // disk
    auto newDictionaryChunk = std::make_unique<DictionaryChunk>(numValues, enableCompression);
    // Each index is replaced by a new one for the de-duplicated data in the new dictionary.
    for (auto i = 0u; i < numValues; i++) {
        if (nullChunk->isNull(i)) {
            continue;
        }
        auto stringData = getValue<std::string_view>(i);
        auto index = newDictionaryChunk->appendString(stringData);
        setValue<DictionaryChunk::string_index_t>(index, i);
    }
    dictionaryChunk = std::move(newDictionaryChunk);
}

template<>
common::ku_string_t StringColumnChunk::getValue<common::ku_string_t>(offset_t) const {
    KU_UNREACHABLE;
}

// STRING
template<>
std::string_view StringColumnChunk::getValue<std::string_view>(offset_t pos) const {
    KU_ASSERT(pos < numValues);
    KU_ASSERT(!nullChunk->isNull(pos));
    auto index = ColumnChunk::getValue<DictionaryChunk::string_index_t>(pos);
    return dictionaryChunk->getString(index);
}

template<>
std::string StringColumnChunk::getValue<std::string>(offset_t pos) const {
    return std::string(getValue<std::string_view>(pos));
}

} // namespace storage
} // namespace kuzu
