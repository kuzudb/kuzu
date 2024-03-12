#include "storage/store/struct_column_chunk.h"

#include "common/data_chunk/sel_vector.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "storage/store/column_chunk.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

// TODO: need to handle this case, when the whole struct entry is null, should set all fields to
// null too.
StructColumnChunk::StructColumnChunk(
    LogicalType dataType, uint64_t capacity, bool enableCompression, bool inMemory)
    : ColumnChunk{std::move(dataType), capacity} {
    auto fieldTypes = StructType::getFieldTypes(&this->dataType);
    childChunks.resize(fieldTypes.size());
    for (auto i = 0u; i < fieldTypes.size(); i++) {
        childChunks[i] = ColumnChunkFactory::createColumnChunk(
            *fieldTypes[i]->copy(), enableCompression, capacity, inMemory);
    }
}

void StructColumnChunk::finalize() {
    for (auto& childChunk : childChunks) {
        childChunk->finalize();
    }
}

void StructColumnChunk::append(
    ColumnChunk* other, offset_t startPosInOtherChunk, uint32_t numValuesToAppend) {
    KU_ASSERT(other->getDataType().getPhysicalType() == PhysicalTypeID::STRUCT);
    auto otherStructChunk = ku_dynamic_cast<ColumnChunk*, StructColumnChunk*>(other);
    KU_ASSERT(childChunks.size() == otherStructChunk->childChunks.size());
    nullChunk->append(other->getNullChunk(), startPosInOtherChunk, numValuesToAppend);
    for (auto i = 0u; i < childChunks.size(); i++) {
        childChunks[i]->append(
            otherStructChunk->childChunks[i].get(), startPosInOtherChunk, numValuesToAppend);
    }
    numValues += numValuesToAppend;
}

void StructColumnChunk::append(ValueVector* vector, SelectionVector& selVector) {
    auto numFields = StructType::getNumFields(&dataType);
    for (auto i = 0u; i < numFields; i++) {
        childChunks[i]->append(StructVector::getFieldVector(vector, i).get(), selVector);
    }
    for (auto i = 0u; i < selVector.selectedSize; i++) {
        nullChunk->setNull(numValues + i, vector->isNull(selVector.selectedPositions[i]));
    }
    numValues += selVector.selectedSize;
}

void StructColumnChunk::resize(uint64_t newCapacity) {
    ColumnChunk::resize(newCapacity);
    capacity = newCapacity;
    for (auto& child : childChunks) {
        child->resize(newCapacity);
    }
}

void StructColumnChunk::resetToEmpty() {
    ColumnChunk::resetToEmpty();
    for (auto& child : childChunks) {
        child->resetToEmpty();
    }
}

void StructColumnChunk::write(
    ValueVector* vector, offset_t offsetInVector, offset_t offsetInChunk) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::STRUCT);
    nullChunk->setNull(offsetInChunk, vector->isNull(offsetInVector));
    auto fields = StructVector::getFieldVectors(vector);
    for (auto i = 0u; i < fields.size(); i++) {
        childChunks[i]->write(fields[i].get(), offsetInVector, offsetInChunk);
    }
    if (offsetInChunk >= numValues) {
        numValues = offsetInChunk + 1;
    }
}

void StructColumnChunk::write(ColumnChunk* chunk, ColumnChunk* dstOffsets, bool isCSR) {
    KU_ASSERT(chunk->getDataType().getPhysicalType() == PhysicalTypeID::STRUCT);
    for (auto i = 0u; i < dstOffsets->getNumValues(); i++) {
        auto offsetInChunk = dstOffsets->getValue<offset_t>(i);
        KU_ASSERT(offsetInChunk < capacity);
        nullChunk->setNull(offsetInChunk, chunk->getNullChunk()->isNull(i));
    }
    auto structChunk = ku_dynamic_cast<ColumnChunk*, StructColumnChunk*>(chunk);
    for (auto i = 0u; i < childChunks.size(); i++) {
        childChunks[i]->write(structChunk->getChild(i), dstOffsets, isCSR);
    }
}

void StructColumnChunk::write(ColumnChunk* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->getDataType().getPhysicalType() == PhysicalTypeID::STRUCT);
    auto srcStructChunk = ku_dynamic_cast<ColumnChunk*, StructColumnChunk*>(srcChunk);
    KU_ASSERT(childChunks.size() == srcStructChunk->childChunks.size());
    nullChunk->write(srcChunk->getNullChunk(), srcOffsetInChunk, dstOffsetInChunk, numValuesToCopy);
    for (auto i = 0u; i < childChunks.size(); i++) {
        childChunks[i]->write(srcStructChunk->childChunks[i].get(), srcOffsetInChunk,
            dstOffsetInChunk, numValuesToCopy);
    }
}

void StructColumnChunk::copy(ColumnChunk* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    while (numValues < dstOffsetInChunk) {
        nullChunk->setNull(numValues, true);
        numValues++;
    }
    nullChunk->append(srcChunk->getNullChunk(), srcOffsetInChunk, numValuesToCopy);
    auto srcStructChunk = ku_dynamic_cast<ColumnChunk*, StructColumnChunk*>(srcChunk);
    for (auto i = 0u; i < childChunks.size(); i++) {
        childChunks[i]->copy(srcStructChunk->childChunks[i].get(), srcOffsetInChunk,
            dstOffsetInChunk, numValuesToCopy);
    }
    numValues += numValuesToCopy;
    KU_ASSERT(nullChunk->getNumValues() == numValues);
}

bool StructColumnChunk::numValuesSanityCheck() const {
    for (auto& child : childChunks) {
        if (child->getNumValues() != numValues) {
            return false;
        }
        if (!child->numValuesSanityCheck()) {
            return false;
        }
    }
    return nullChunk->getNumValues() == numValues;
}

} // namespace storage
} // namespace kuzu
