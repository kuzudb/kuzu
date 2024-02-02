#include "storage/store/struct_column_chunk.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

// TODO: need to handle this case, when the whole struct entry is null, should set all fields to
// null too.
StructColumnChunk::StructColumnChunk(
    std::unique_ptr<LogicalType> dataType, uint64_t capacity, bool enableCompression)
    : ColumnChunk{std::move(dataType), capacity} {
    auto fieldTypes = StructType::getFieldTypes(this->dataType.get());
    childChunks.resize(fieldTypes.size());
    for (auto i = 0u; i < fieldTypes.size(); i++) {
        childChunks[i] = ColumnChunkFactory::createColumnChunk(
            fieldTypes[i]->copy(), enableCompression, capacity);
    }
}

void StructColumnChunk::finalize() {
    for (auto& childChunk : childChunks) {
        childChunk->finalize();
    }
}

void StructColumnChunk::append(
    ColumnChunk* other, offset_t startPosInOtherChunk, uint32_t numValuesToAppend) {
    KU_ASSERT(other->getDataType()->getPhysicalType() == PhysicalTypeID::STRUCT);
    auto otherStructChunk = ku_dynamic_cast<ColumnChunk*, StructColumnChunk*>(other);
    KU_ASSERT(childChunks.size() == otherStructChunk->childChunks.size());
    nullChunk->append(other->getNullChunk(), startPosInOtherChunk, numValuesToAppend);
    for (auto i = 0u; i < childChunks.size(); i++) {
        childChunks[i]->append(
            otherStructChunk->childChunks[i].get(), startPosInOtherChunk, numValuesToAppend);
    }
    numValues += numValuesToAppend;
}

void StructColumnChunk::append(ValueVector* vector) {
    auto numFields = StructType::getNumFields(dataType.get());
    for (auto i = 0u; i < numFields; i++) {
        childChunks[i]->append(StructVector::getFieldVector(vector, i).get());
    }
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        nullChunk->setNull(
            numValues + i, vector->isNull(vector->state->selVector->selectedPositions[i]));
    }
    numValues += vector->state->selVector->selectedSize;
}

void StructColumnChunk::resize(uint64_t newCapacity) {
    ColumnChunk::resize(newCapacity);
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

void StructColumnChunk::write(
    ValueVector* valueVector, ValueVector* offsetInChunkVector, bool isCSR) {
    KU_ASSERT(valueVector->dataType.getPhysicalType() == PhysicalTypeID::STRUCT);
    auto offsets = reinterpret_cast<offset_t*>(offsetInChunkVector->getData());
    for (auto i = 0u; i < offsetInChunkVector->state->selVector->selectedSize; i++) {
        auto offsetInChunk = offsets[offsetInChunkVector->state->selVector->selectedPositions[i]];
        KU_ASSERT(offsetInChunk < capacity);
        nullChunk->setNull(offsetInChunk, valueVector->isNull(i));
    }
    auto fields = StructVector::getFieldVectors(valueVector);
    for (auto i = 0u; i < fields.size(); i++) {
        childChunks[i]->write(fields[i].get(), offsetInChunkVector, isCSR);
    }
}

void StructColumnChunk::write(ColumnChunk* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->getDataType()->getPhysicalType() == PhysicalTypeID::STRUCT);
    auto srcStructChunk = ku_dynamic_cast<ColumnChunk*, StructColumnChunk*>(srcChunk);
    KU_ASSERT(childChunks.size() == srcStructChunk->childChunks.size());
    nullChunk->write(srcChunk->getNullChunk(), srcOffsetInChunk, dstOffsetInChunk, numValuesToCopy);
    for (auto i = 0u; i < childChunks.size(); i++) {
        childChunks[i]->write(srcStructChunk->childChunks[i].get(), srcOffsetInChunk,
            dstOffsetInChunk, numValuesToCopy);
    }
}

} // namespace storage
} // namespace kuzu
