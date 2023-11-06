#include "storage/store/struct_column_chunk.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

StructColumnChunk::StructColumnChunk(
    LogicalType dataType, uint64_t capacity, bool enableCompression)
    : ColumnChunk{std::move(dataType), capacity} {
    auto fieldTypes = StructType::getFieldTypes(&this->dataType);
    childChunks.resize(fieldTypes.size());
    for (auto i = 0u; i < fieldTypes.size(); i++) {
        childChunks[i] =
            ColumnChunkFactory::createColumnChunk(*fieldTypes[i], capacity, enableCompression);
    }
}

void StructColumnChunk::append(ColumnChunk* other, offset_t startPosInOtherChunk,
    offset_t startPosInChunk, uint32_t numValuesToAppend) {
    auto otherStructChunk = dynamic_cast<StructColumnChunk*>(other);
    nullChunk->append(
        other->getNullChunk(), startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    for (auto i = 0u; i < childChunks.size(); i++) {
        childChunks[i]->append(otherStructChunk->childChunks[i].get(), startPosInOtherChunk,
            startPosInChunk, numValuesToAppend);
    }
    numValues += numValuesToAppend;
}

void StructColumnChunk::append(ValueVector* vector, offset_t startPosInChunk) {
    auto numFields = StructType::getNumFields(&dataType);
    for (auto i = 0u; i < numFields; i++) {
        childChunks[i]->append(StructVector::getFieldVector(vector, i).get(), startPosInChunk);
    }
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        nullChunk->setNull(
            startPosInChunk + i, vector->isNull(vector->state->selVector->selectedPositions[i]));
    }
    numValues += vector->state->selVector->selectedSize;
}

void StructColumnChunk::resize(uint64_t newCapacity) {
    ColumnChunk::resize(newCapacity);
    for (auto& child : childChunks) {
        child->resize(newCapacity);
    }
}

void StructColumnChunk::write(ValueVector* vector, offset_t startOffsetInChunk) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::STRUCT);
    auto fields = StructVector::getFieldVectors(vector);
    for (auto i = 0u; i < fields.size(); i++) {
        childChunks[i]->write(fields[i].get(), startOffsetInChunk);
    }
}

void StructColumnChunk::write(ValueVector* valueVector, ValueVector* offsetInChunkVector) {
    KU_ASSERT(valueVector->dataType.getPhysicalType() == PhysicalTypeID::STRUCT);
    auto fields = StructVector::getFieldVectors(valueVector);
    for (auto i = 0u; i < fields.size(); i++) {
        childChunks[i]->write(fields[i].get(), offsetInChunkVector);
    }
}

} // namespace storage
} // namespace kuzu
