#include "storage/store/struct_chunk_data.h"

#include "common/data_chunk/sel_vector.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/store/column_chunk_data.h"
#include "storage/store/struct_column.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

StructChunkData::StructChunkData(MemoryManager& mm, LogicalType dataType, uint64_t capacity,
    bool enableCompression, ResidencyState residencyState)
    : ColumnChunkData{mm, std::move(dataType), capacity, enableCompression, residencyState,
          true /*hasNullData*/} {
    const auto fieldTypes = StructType::getFieldTypes(this->dataType);
    childChunks.resize(fieldTypes.size());
    for (auto i = 0u; i < fieldTypes.size(); i++) {
        childChunks[i] = ColumnChunkFactory::createColumnChunkData(mm, fieldTypes[i]->copy(),
            enableCompression, capacity, residencyState);
    }
}

StructChunkData::StructChunkData(MemoryManager& mm, LogicalType dataType, bool enableCompression,
    const ColumnChunkMetadata& metadata)
    : ColumnChunkData{mm, std::move(dataType), enableCompression, metadata, true /*hasNullData*/} {
    const auto fieldTypes = StructType::getFieldTypes(this->dataType);
    childChunks.resize(fieldTypes.size());
    for (auto i = 0u; i < fieldTypes.size(); i++) {
        childChunks[i] = ColumnChunkFactory::createColumnChunkData(mm, fieldTypes[i]->copy(),
            enableCompression, 0, ResidencyState::IN_MEMORY);
    }
}

void StructChunkData::finalize() {
    for (const auto& childChunk : childChunks) {
        childChunk->finalize();
    }
}

uint64_t StructChunkData::getEstimatedMemoryUsage() const {
    auto estimatedMemoryUsage = ColumnChunkData::getEstimatedMemoryUsage();
    for (auto& childChunk : childChunks) {
        estimatedMemoryUsage += childChunk->getEstimatedMemoryUsage();
    }
    return estimatedMemoryUsage;
}

void StructChunkData::resetNumValuesFromMetadata() {
    ColumnChunkData::resetNumValuesFromMetadata();
    for (const auto& childChunk : childChunks) {
        childChunk->resetNumValuesFromMetadata();
    }
}

void StructChunkData::resetToAllNull() {
    ColumnChunkData::resetToAllNull();
    for (const auto& childChunk : childChunks) {
        childChunk->resetToAllNull();
    }
}

void StructChunkData::serialize(Serializer& serializer) const {
    ColumnChunkData::serialize(serializer);
    serializer.writeDebuggingInfo("struct_children");
    serializer.serializeVectorOfPtrs<ColumnChunkData>(childChunks);
}

void StructChunkData::deserialize(Deserializer& deSer, ColumnChunkData& chunkData) {
    std::string key;
    deSer.validateDebuggingInfo(key, "struct_children");
    deSer.deserializeVectorOfPtrs<ColumnChunkData>(chunkData.cast<StructChunkData>().childChunks,
        [&](Deserializer& deser) {
            return ColumnChunkData::deserialize(chunkData.getMemoryManager(), deser);
        });
}

void StructChunkData::flush(FileHandle& dataFH) {
    ColumnChunkData::flush(dataFH);
    for (const auto& childChunk : childChunks) {
        childChunk->flush(dataFH);
    }
}

void StructChunkData::append(ColumnChunkData* other, offset_t startPosInOtherChunk,
    uint32_t numValuesToAppend) {
    KU_ASSERT(other->getDataType().getPhysicalType() == PhysicalTypeID::STRUCT);
    const auto& otherStructChunk = other->cast<StructChunkData>();
    KU_ASSERT(childChunks.size() == otherStructChunk.childChunks.size());
    nullData->append(other->getNullData(), startPosInOtherChunk, numValuesToAppend);
    for (auto i = 0u; i < childChunks.size(); i++) {
        childChunks[i]->append(otherStructChunk.childChunks[i].get(), startPosInOtherChunk,
            numValuesToAppend);
    }
    numValues += numValuesToAppend;
}

void StructChunkData::append(ValueVector* vector, const SelectionVector& selVector) {
    const auto numFields = StructType::getNumFields(dataType);
    for (auto i = 0u; i < numFields; i++) {
        childChunks[i]->append(StructVector::getFieldVector(vector, i).get(), selVector);
    }
    for (auto i = 0u; i < selVector.getSelSize(); i++) {
        nullData->setNull(numValues + i, vector->isNull(selVector[i]));
    }
    numValues += selVector.getSelSize();
}

void StructChunkData::scan(ValueVector& output, offset_t offset, length_t length,
    sel_t posInOutputVector) const {
    KU_ASSERT(offset + length <= numValues);
    if (nullData) {
        nullData->scan(output, offset, length, posInOutputVector);
    }
    const auto numFields = StructType::getNumFields(dataType);
    for (auto i = 0u; i < numFields; i++) {
        childChunks[i]->scan(*StructVector::getFieldVector(&output, i), offset, length,
            posInOutputVector);
    }
}

void StructChunkData::lookup(offset_t offsetInChunk, ValueVector& output,
    sel_t posInOutputVector) const {
    KU_ASSERT(offsetInChunk < numValues);
    const auto numFields = StructType::getNumFields(dataType);
    output.setNull(posInOutputVector, nullData->isNull(offsetInChunk));
    for (auto i = 0u; i < numFields; i++) {
        childChunks[i]->lookup(offsetInChunk, *StructVector::getFieldVector(&output, i).get(),
            posInOutputVector);
    }
}

void StructChunkData::initializeScanState(ChunkState& state, Column* column) const {
    ColumnChunkData::initializeScanState(state, column);

    auto* structColumn = ku_dynamic_cast<Column*, StructColumn*>(column);
    state.childrenStates.resize(childChunks.size());
    for (auto i = 0u; i < childChunks.size(); i++) {
        childChunks[i]->initializeScanState(state.childrenStates[i], structColumn->getChild(i));
    }
}

void StructChunkData::setToInMemory() {
    ColumnChunkData::setToInMemory();
    for (const auto& child : childChunks) {
        child->setToInMemory();
    }
}

void StructChunkData::resize(uint64_t newCapacity) {
    ColumnChunkData::resize(newCapacity);
    capacity = newCapacity;
    for (const auto& child : childChunks) {
        child->resize(newCapacity);
    }
}

void StructChunkData::resetToEmpty() {
    ColumnChunkData::resetToEmpty();
    for (const auto& child : childChunks) {
        child->resetToEmpty();
    }
}

void StructChunkData::write(const ValueVector* vector, offset_t offsetInVector,
    offset_t offsetInChunk) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::STRUCT);
    nullData->setNull(offsetInChunk, vector->isNull(offsetInVector));
    const auto fields = StructVector::getFieldVectors(vector);
    for (auto i = 0u; i < fields.size(); i++) {
        childChunks[i]->write(fields[i].get(), offsetInVector, offsetInChunk);
    }
    if (offsetInChunk >= numValues) {
        numValues = offsetInChunk + 1;
    }
}

void StructChunkData::write(ColumnChunkData* chunk, ColumnChunkData* dstOffsets,
    RelMultiplicity multiplicity) {
    KU_ASSERT(chunk->getDataType().getPhysicalType() == PhysicalTypeID::STRUCT &&
              dstOffsets->getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
    for (auto i = 0u; i < dstOffsets->getNumValues(); i++) {
        const auto offsetInChunk = dstOffsets->getValue<offset_t>(i);
        KU_ASSERT(offsetInChunk < capacity);
        nullData->setNull(offsetInChunk, chunk->getNullData()->isNull(i));
        numValues = offsetInChunk >= numValues ? offsetInChunk + 1 : numValues;
    }
    auto& structChunk = chunk->cast<StructChunkData>();
    for (auto i = 0u; i < childChunks.size(); i++) {
        childChunks[i]->write(structChunk.getChild(i), dstOffsets, multiplicity);
    }
}

void StructChunkData::write(ColumnChunkData* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->getDataType().getPhysicalType() == PhysicalTypeID::STRUCT);
    const auto& srcStructChunk = srcChunk->cast<StructChunkData>();
    KU_ASSERT(childChunks.size() == srcStructChunk.childChunks.size());
    nullData->write(srcChunk->getNullData(), srcOffsetInChunk, dstOffsetInChunk, numValuesToCopy);
    if ((dstOffsetInChunk + numValuesToCopy) >= numValues) {
        numValues = dstOffsetInChunk + numValuesToCopy;
    }
    for (auto i = 0u; i < childChunks.size(); i++) {
        childChunks[i]->write(srcStructChunk.childChunks[i].get(), srcOffsetInChunk,
            dstOffsetInChunk, numValuesToCopy);
    }
}

void StructChunkData::copy(ColumnChunkData* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    while (numValues < dstOffsetInChunk) {
        nullData->setNull(numValues, true);
        numValues++;
    }
    nullData->append(srcChunk->getNullData(), srcOffsetInChunk, numValuesToCopy);
    auto& srcStructChunk = srcChunk->cast<StructChunkData>();
    for (auto i = 0u; i < childChunks.size(); i++) {
        childChunks[i]->copy(srcStructChunk.childChunks[i].get(), srcOffsetInChunk,
            dstOffsetInChunk, numValuesToCopy);
    }
    numValues += numValuesToCopy;
    KU_ASSERT(nullData->getNumValues() == numValues);
}

bool StructChunkData::numValuesSanityCheck() const {
    for (auto& child : childChunks) {
        if (child->getNumValues() != numValues) {
            return false;
        }
        if (!child->numValuesSanityCheck()) {
            return false;
        }
    }
    return nullData->getNumValues() == numValues;
}

} // namespace storage
} // namespace kuzu
