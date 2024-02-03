#include "storage/store/internal_id_column_chunk.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

InternalIDColumnChunk::InternalIDColumnChunk(
    uint64_t capacity, std::unique_ptr<LogicalType> dataType, bool enableCompression)
    : StructColumnChunk(std::move(dataType), capacity, enableCompression) {}

void InternalIDColumnChunk::append(common::ValueVector* vector) {
    KU_ASSERT(vector->dataType.getLogicalTypeID() == LogicalTypeID::STRUCT &&
              vector->dataType.getPhysicalType() == PhysicalTypeID::STRUCT);
    StructColumnChunk::append(vector);
}

void InternalIDColumnChunk::write(
    ValueVector* vector, offset_t offsetInVector, offset_t offsetInChunk) {
    KU_ASSERT(vector->dataType.getLogicalTypeID() == LogicalTypeID::STRUCT);
    StructColumnChunk::write(vector, offsetInVector, offsetInChunk);
}

void InternalIDColumnChunk::write(
    ValueVector* valueVector, ValueVector* offsetInChunkVector, bool isCSR) {
    KU_ASSERT(offsetInChunkVector->dataType.getPhysicalType() == PhysicalTypeID::INT64);
    KU_ASSERT(valueVector->dataType.getPhysicalType() == PhysicalTypeID::STRUCT);
    StructColumnChunk::write(valueVector, offsetInChunkVector, isCSR);
}

void InternalIDColumnChunk::append(
    ColumnChunk* other, common::offset_t startPosInOtherChunk, uint32_t numValuesToAppend) {
    KU_ASSERT(other->getDataType()->getLogicalTypeID() == LogicalTypeID::STRUCT &&
              other->getDataType()->getPhysicalType() == PhysicalTypeID::STRUCT);
    StructColumnChunk::append(other, startPosInOtherChunk, numValuesToAppend);
}

} // namespace storage
} // namespace kuzu
