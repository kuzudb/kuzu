#include "storage/store/column_chunk_data.h"

#include <algorithm>
#include <cstring>

#include "common/data_chunk/sel_vector.h"
#include "common/exception/copy.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/type_utils.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "expression_evaluator/expression_evaluator.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/compression/compression.h"
#include "storage/compression/float_compression.h"
#include "storage/store/column.h"
#include "storage/store/column_chunk_metadata.h"
#include "storage/store/compression_flush_buffer.h"
#include "storage/store/list_chunk_data.h"
#include "storage/store/string_chunk_data.h"
#include "storage/store/struct_chunk_data.h"
#include <ranges>

using namespace kuzu::common;
using namespace kuzu::evaluator;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

static std::shared_ptr<CompressionAlg> getCompression(const LogicalType& dataType,
    bool enableCompression) {
    if (!enableCompression) {
        return std::make_shared<Uncompressed>(dataType);
    }
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::INT128: {
        return std::make_shared<IntegerBitpacking<int128_t>>();
    }
    case PhysicalTypeID::INT64: {
        return std::make_shared<IntegerBitpacking<int64_t>>();
    }
    case PhysicalTypeID::INT32: {
        return std::make_shared<IntegerBitpacking<int32_t>>();
    }
    case PhysicalTypeID::INT16: {
        return std::make_shared<IntegerBitpacking<int16_t>>();
    }
    case PhysicalTypeID::INT8: {
        return std::make_shared<IntegerBitpacking<int8_t>>();
    }
    case PhysicalTypeID::INTERNAL_ID:
    case PhysicalTypeID::UINT64: {
        return std::make_shared<IntegerBitpacking<uint64_t>>();
    }
    case PhysicalTypeID::UINT32: {
        return std::make_shared<IntegerBitpacking<uint32_t>>();
    }
    case PhysicalTypeID::UINT16: {
        return std::make_shared<IntegerBitpacking<uint16_t>>();
    }
    case PhysicalTypeID::UINT8: {
        return std::make_shared<IntegerBitpacking<uint8_t>>();
    }
    case PhysicalTypeID::FLOAT: {
        return std::make_shared<FloatCompression<float>>();
    }
    case PhysicalTypeID::DOUBLE: {
        return std::make_shared<FloatCompression<double>>();
    }
    default: {
        return std::make_shared<Uncompressed>(dataType);
    }
    }
}

ColumnChunkData::ColumnChunkData(MemoryManager& mm, LogicalType dataType, uint64_t capacity,
    bool enableCompression, ResidencyState residencyState, bool hasNullData)
    : residencyState{residencyState}, dataType{std::move(dataType)},
      enableCompression{enableCompression},
      numBytesPerValue{getDataTypeSizeInChunk(this->dataType)}, capacity{capacity}, numValues{0} {
    if (hasNullData) {
        nullData = std::make_unique<NullChunkData>(mm, capacity, enableCompression, residencyState);
    }
    initializeBuffer(this->dataType.getPhysicalType(), mm);
    initializeFunction(enableCompression);
}

ColumnChunkData::ColumnChunkData(MemoryManager& mm, LogicalType dataType, bool enableCompression,
    const ColumnChunkMetadata& metadata, bool hasNullData)
    : residencyState(ResidencyState::ON_DISK), dataType{std::move(dataType)},
      enableCompression{enableCompression},
      numBytesPerValue{getDataTypeSizeInChunk(this->dataType)}, capacity{0},
      numValues{metadata.numValues}, metadata{metadata} {
    if (hasNullData) {
        nullData = std::make_unique<NullChunkData>(mm, enableCompression, metadata);
    }
    initializeBuffer(this->dataType.getPhysicalType(), mm);
    initializeFunction(enableCompression);
}

ColumnChunkData::ColumnChunkData(MemoryManager& mm, PhysicalTypeID dataType, bool enableCompression,
    const ColumnChunkMetadata& metadata, bool hasNullData)
    : ColumnChunkData(mm, LogicalType::ANY(dataType), enableCompression, metadata, hasNullData) {}

void ColumnChunkData::initializeBuffer(common::PhysicalTypeID physicalType, MemoryManager& mm) {
    numBytesPerValue = getDataTypeSizeInChunk(physicalType);

    // Some columnChunks are much smaller than the 256KB minimum size used by allocateBuffer
    // Which would lead to excessive memory use, particularly in the partitioner
    buffer = mm.mallocBuffer(true, getBufferSize(capacity));
}

void ColumnChunkData::initializeFunction(bool enableCompression) {
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        // Since we compress into memory, storage is the same as fixed-sized
        // values, but we need to mark it as being boolean compressed.
        flushBufferFunction = uncompressedFlushBuffer;
        getMetadataFunction = booleanGetMetadata;
    } break;
    case PhysicalTypeID::STRING:
    case PhysicalTypeID::INT64:
    case PhysicalTypeID::INT32:
    case PhysicalTypeID::INT16:
    case PhysicalTypeID::INT8:
    case PhysicalTypeID::INTERNAL_ID:
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST:
    case PhysicalTypeID::UINT64:
    case PhysicalTypeID::UINT32:
    case PhysicalTypeID::UINT16:
    case PhysicalTypeID::UINT8:
    case PhysicalTypeID::INT128: {
        const auto compression = getCompression(dataType, enableCompression);
        flushBufferFunction = CompressedFlushBuffer(compression, dataType);
        getMetadataFunction = GetBitpackingMetadata(compression, dataType);
    } break;
    case PhysicalTypeID::DOUBLE: {
        const auto compression = getCompression(dataType, enableCompression);
        flushBufferFunction = CompressedFloatFlushBuffer<double>(compression, dataType);
        getMetadataFunction = GetFloatCompressionMetadata<double>(compression, dataType);
        break;
    }
    case PhysicalTypeID::FLOAT: {
        const auto compression = getCompression(dataType, enableCompression);
        flushBufferFunction = CompressedFloatFlushBuffer<float>(compression, dataType);
        getMetadataFunction = GetFloatCompressionMetadata<float>(compression, dataType);
        break;
    }
    default: {
        flushBufferFunction = uncompressedFlushBuffer;
        getMetadataFunction = uncompressedGetMetadata;
    }
    }
}

void ColumnChunkData::resetToAllNull() {
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
    if (nullData) {
        nullData->resetToAllNull();
    }
}

void ColumnChunkData::resetToEmpty() {
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
    if (nullData) {
        nullData->resetToEmpty();
    }
    KU_ASSERT(buffer->buffer.size_bytes() == getBufferSize(capacity));
    memset(getData<uint8_t>(), 0x00, buffer->buffer.size_bytes());
    numValues = 0;
}

ColumnChunkMetadata ColumnChunkData::getMetadataToFlush() const {
    KU_ASSERT(numValues <= capacity);
    StorageValue minValue = {}, maxValue = {};
    if (capacity > 0) {
        std::optional<NullMask> nullMask;
        if (nullData) {
            nullMask = nullData->getNullMask();
        }
        auto [min, max] =
            getMinMaxStorageValue(getData(), 0 /*offset*/, numValues, dataType.getPhysicalType(),
                nullMask.has_value() ? &*nullMask : nullptr, true /*valueRequiredIfUnsupported*/);
        minValue = min.value_or(StorageValue());
        maxValue = max.value_or(StorageValue());
    }
    KU_ASSERT(buffer->buffer.size_bytes() == getBufferSize(capacity));
    return getMetadataFunction(buffer->buffer, capacity, numValues, minValue, maxValue);
}

void ColumnChunkData::append(ValueVector* vector, const SelectionVector& selVector) {
    KU_ASSERT(vector->dataType.getPhysicalType() == dataType.getPhysicalType());
    copyVectorToBuffer(vector, numValues, selVector);
    numValues += selVector.getSelSize();
}

void ColumnChunkData::append(ColumnChunkData* other, offset_t startPosInOtherChunk,
    uint32_t numValuesToAppend) {
    KU_ASSERT(other->dataType.getPhysicalType() == dataType.getPhysicalType());
    if (nullData) {
        KU_ASSERT(nullData->getNumValues() == getNumValues());
        nullData->append(other->nullData.get(), startPosInOtherChunk, numValuesToAppend);
    }
    KU_ASSERT(numValues + numValuesToAppend <= capacity);
    memcpy(getData<uint8_t>() + numValues * numBytesPerValue,
        other->getData<uint8_t>() + startPosInOtherChunk * numBytesPerValue,
        numValuesToAppend * numBytesPerValue);
    numValues += numValuesToAppend;
}

void ColumnChunkData::flush(FileHandle& dataFH) {
    const auto preScanMetadata = getMetadataToFlush();
    const auto startPageIdx = dataFH.addNewPages(preScanMetadata.numPages);
    const auto metadata = flushBuffer(&dataFH, startPageIdx, preScanMetadata);
    setToOnDisk(metadata);
    if (nullData) {
        nullData->flush(dataFH);
    }
}

// Note: This function is not setting child/null chunk data recursively.
void ColumnChunkData::setToOnDisk(const ColumnChunkMetadata& metadata) {
    residencyState = ResidencyState::ON_DISK;
    capacity = 0;
    // Note: We don't need to set the buffer to nullptr, as it allows ColumnChunkDaat to be resized.
    buffer = buffer->mm->mallocBuffer(true, 0 /*size*/);
    this->metadata = metadata;
    this->numValues = metadata.numValues;
}

ColumnChunkMetadata ColumnChunkData::flushBuffer(FileHandle* dataFH, page_idx_t startPageIdx,
    const ColumnChunkMetadata& metadata) const {
    if (!metadata.compMeta.isConstant() && getBufferSize() != 0) {
        KU_ASSERT(getBufferSize() == getBufferSize(capacity));
        return flushBufferFunction(buffer->buffer, dataFH, startPageIdx, metadata);
    }
    return metadata;
}

uint64_t ColumnChunkData::getBufferSize(uint64_t capacity_) const {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        // 8 values per byte, and we need a buffer size which is a
        // multiple of 8 bytes.
        return ceil(capacity_ / 8.0 / 8.0) * 8;
    }
    default: {
        return numBytesPerValue * capacity_;
    }
    }
}

void ColumnChunkData::initializeScanState(ChunkState& state, Column* column) const {
    if (nullData) {
        KU_ASSERT(state.nullState);
        nullData->initializeScanState(*state.nullState, column->getNullColumn());
    }
    state.column = column;
    if (residencyState == ResidencyState::ON_DISK) {
        state.metadata = metadata;
        state.numValuesPerPage = state.metadata.compMeta.numValues(PAGE_SIZE, dataType);

        state.column->populateExtraChunkState(state);
    }
}

void ColumnChunkData::scan(ValueVector& output, offset_t offset, length_t length,
    sel_t posInOutputVector) const {
    KU_ASSERT(offset + length <= numValues);
    if (nullData) {
        nullData->scan(output, offset, length, posInOutputVector);
    }
    memcpy(output.getData() + posInOutputVector * numBytesPerValue,
        getData() + offset * numBytesPerValue, numBytesPerValue * length);
}

void ColumnChunkData::lookup(offset_t offsetInChunk, ValueVector& output,
    sel_t posInOutputVector) const {
    KU_ASSERT(offsetInChunk < capacity);
    output.setNull(posInOutputVector, isNull(offsetInChunk));
    if (!output.isNull(posInOutputVector)) {
        memcpy(output.getData() + posInOutputVector * numBytesPerValue,
            getData() + offsetInChunk * numBytesPerValue, numBytesPerValue);
    }
}

void ColumnChunkData::write(ColumnChunkData* chunk, ColumnChunkData* dstOffsets,
    RelMultiplicity multiplicity) {
    KU_ASSERT(chunk->dataType.getPhysicalType() == dataType.getPhysicalType() &&
              dstOffsets->getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID &&
              chunk->getNumValues() == dstOffsets->getNumValues());
    for (auto i = 0u; i < dstOffsets->getNumValues(); i++) {
        const auto dstOffset = dstOffsets->getValue<offset_t>(i);
        KU_ASSERT(dstOffset < capacity);
        memcpy(getData() + dstOffset * numBytesPerValue, chunk->getData() + i * numBytesPerValue,
            numBytesPerValue);
        numValues = dstOffset >= numValues ? dstOffset + 1 : numValues;
    }
    if (nullData || multiplicity == RelMultiplicity::ONE) {
        for (auto i = 0u; i < dstOffsets->getNumValues(); i++) {
            const auto dstOffset = dstOffsets->getValue<offset_t>(i);
            if (multiplicity == RelMultiplicity::ONE && isNull(dstOffset)) {
                throw CopyException(
                    stringFormat("Node with offset: {} can only have one neighbour due "
                                 "to the MANY-ONE/ONE-ONE relationship constraint.",
                        dstOffset));
            }
            if (nullData) {
                nullData->setNull(dstOffset, chunk->isNull(i));
            }
        }
    }
}

// NOTE: This function is only called in LocalTable right now when
// performing out-of-place committing. LIST has a different logic for
// handling out-of-place committing as it has to be slided. However,
// this is unsafe, as this function can also be used for other purposes
// later. Thus, an assertion is added at the first line.
void ColumnChunkData::write(const ValueVector* vector, offset_t offsetInVector,
    offset_t offsetInChunk) {
    KU_ASSERT(dataType.getPhysicalType() != PhysicalTypeID::BOOL &&
              dataType.getPhysicalType() != PhysicalTypeID::LIST &&
              dataType.getPhysicalType() != PhysicalTypeID::ARRAY);
    if (nullData) {
        nullData->setNull(offsetInChunk, vector->isNull(offsetInVector));
    }
    if (offsetInChunk >= numValues) {
        numValues = offsetInChunk + 1;
    }
    if (!vector->isNull(offsetInVector)) {
        memcpy(getData() + offsetInChunk * numBytesPerValue,
            vector->getData() + offsetInVector * numBytesPerValue, numBytesPerValue);
    }
}

void ColumnChunkData::write(ColumnChunkData* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->dataType.getPhysicalType() == dataType.getPhysicalType());
    if ((dstOffsetInChunk + numValuesToCopy) >= numValues) {
        numValues = dstOffsetInChunk + numValuesToCopy;
    }
    memcpy(getData() + dstOffsetInChunk * numBytesPerValue,
        srcChunk->getData() + srcOffsetInChunk * numBytesPerValue,
        numValuesToCopy * numBytesPerValue);
    if (nullData) {
        KU_ASSERT(srcChunk->getNullData());
        nullData->write(srcChunk->getNullData(), srcOffsetInChunk, dstOffsetInChunk,
            numValuesToCopy);
    }
}

void ColumnChunkData::copy(ColumnChunkData* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->dataType.getPhysicalType() == dataType.getPhysicalType());
    KU_ASSERT(dstOffsetInChunk >= numValues);
    KU_ASSERT(dstOffsetInChunk < capacity);
    if (nullData) {
        while (numValues < dstOffsetInChunk) {
            nullData->setNull(numValues, true);
            numValues++;
        }
    } else {
        if (numValues < dstOffsetInChunk) {
            numValues = dstOffsetInChunk;
        }
    }
    append(srcChunk, srcOffsetInChunk, numValuesToCopy);
}

void ColumnChunkData::resetNumValuesFromMetadata() {
    KU_ASSERT(residencyState == ResidencyState::ON_DISK);
    numValues = metadata.numValues;
    if (nullData) {
        nullData->resetNumValuesFromMetadata();
    }
}

void ColumnChunkData::setToInMemory() {
    KU_ASSERT(residencyState == ResidencyState::ON_DISK);
    KU_ASSERT(capacity == 0 && getBufferSize() == 0);
    residencyState = ResidencyState::IN_MEMORY;
    numValues = 0;
    if (nullData) {
        nullData->setToInMemory();
    }
}

void ColumnChunkData::resize(uint64_t newCapacity) {
    if (newCapacity > capacity) {
        capacity = newCapacity;
    }
    const auto numBytesAfterResize = getBufferSize(newCapacity);
    if (numBytesAfterResize > getBufferSize()) {
        auto resizedBuffer = buffer->mm->mallocBuffer(true, numBytesAfterResize);
        memcpy(resizedBuffer->buffer.data(), buffer->buffer.data(), getBufferSize());
        buffer = std::move(resizedBuffer);
    }
    if (nullData) {
        nullData->resize(newCapacity);
    }
}

void ColumnChunkData::populateWithDefaultVal(ExpressionEvaluator& defaultEvaluator,
    uint64_t& numValues_) {
    auto numValuesAppended = 0u;
    const auto numValuesToPopulate = numValues_;
    while (numValuesAppended < numValuesToPopulate) {
        const auto numValuesToAppend =
            std::min(DEFAULT_VECTOR_CAPACITY, numValuesToPopulate - numValuesAppended);
        defaultEvaluator.getLocalStateUnsafe().count = numValuesToAppend;
        defaultEvaluator.evaluate();
        auto resultVector = defaultEvaluator.resultVector.get();
        KU_ASSERT(resultVector->state->getSelVector().getSelSize() == numValuesToAppend);
        append(resultVector, resultVector->state->getSelVector());
        numValuesAppended += numValuesToAppend;
    }
}

void ColumnChunkData::copyVectorToBuffer(ValueVector* vector, offset_t startPosInChunk,
    const SelectionVector& selVector) {
    auto bufferToWrite = buffer->buffer.data() + startPosInChunk * numBytesPerValue;
    KU_ASSERT(startPosInChunk + selVector.getSelSize() <= capacity);
    const auto vectorDataToWriteFrom = vector->getData();
    if (selVector.isUnfiltered()) {
        memcpy(bufferToWrite, vectorDataToWriteFrom, selVector.getSelSize() * numBytesPerValue);
        if (nullData) {
            // TODO(Guodong): Should be wrapped into nullChunk->append(vector);
            for (auto i = 0u; i < selVector.getSelSize(); i++) {
                nullData->setNull(startPosInChunk + i, vector->isNull(i));
            }
        }
    } else {
        for (auto i = 0u; i < selVector.getSelSize(); i++) {
            const auto pos = selVector[i];
            memcpy(bufferToWrite, vectorDataToWriteFrom + pos * numBytesPerValue, numBytesPerValue);
            bufferToWrite += numBytesPerValue;
        }
        if (nullData) {
            // TODO(Guodong): Should be wrapped into nullChunk->append(vector);
            for (auto i = 0u; i < selVector.getSelSize(); i++) {
                const auto pos = selVector[i];
                nullData->setNull(startPosInChunk + i, vector->isNull(pos));
            }
        }
    }
}

void ColumnChunkData::setNumValues(uint64_t numValues_) {
    KU_ASSERT(numValues_ <= capacity);
    numValues = numValues_;
    if (nullData) {
        nullData->setNumValues(numValues_);
    }
}

bool ColumnChunkData::numValuesSanityCheck() const {
    if (nullData) {
        return numValues == nullData->getNumValues();
    }
    return numValues <= capacity;
}

bool ColumnChunkData::sanityCheck() const {
    if (nullData) {
        return nullData->sanityCheck() && numValuesSanityCheck();
    }
    return numValues <= capacity;
}

uint64_t ColumnChunkData::getEstimatedMemoryUsage() const {
    return buffer->buffer.size() + (nullData ? nullData->getEstimatedMemoryUsage() : 0);
}

void ColumnChunkData::serialize(Serializer& serializer) const {
    KU_ASSERT(residencyState == ResidencyState::ON_DISK);
    serializer.writeDebuggingInfo("data_type");
    dataType.serialize(serializer);
    serializer.writeDebuggingInfo("metadata");
    metadata.serialize(serializer);
    serializer.writeDebuggingInfo("enable_compression");
    serializer.write<bool>(enableCompression);
    serializer.writeDebuggingInfo("has_null");
    serializer.write<bool>(nullData != nullptr);
    if (nullData) {
        serializer.writeDebuggingInfo("null_data");
        nullData->serialize(serializer);
    }
}

std::unique_ptr<ColumnChunkData> ColumnChunkData::deserialize(MemoryManager& memoryManager,
    Deserializer& deSer) {
    std::string key;
    ColumnChunkMetadata metadata;
    bool enableCompression = false;
    bool hasNull = false;
    deSer.validateDebuggingInfo(key, "data_type");
    const auto dataType = LogicalType::deserialize(deSer);
    deSer.validateDebuggingInfo(key, "metadata");
    metadata = decltype(metadata)::deserialize(deSer);
    deSer.validateDebuggingInfo(key, "enable_compression");
    deSer.deserializeValue<bool>(enableCompression);
    deSer.validateDebuggingInfo(key, "has_null");
    deSer.deserializeValue<bool>(hasNull);
    auto chunkData = ColumnChunkFactory::createColumnChunkData(memoryManager, dataType.copy(),
        enableCompression, metadata, hasNull);
    if (hasNull) {
        deSer.validateDebuggingInfo(key, "null_data");
        chunkData->nullData = NullChunkData::deserialize(memoryManager, deSer);
    }

    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::STRUCT: {
        StructChunkData::deserialize(deSer, *chunkData);
    } break;
    case PhysicalTypeID::STRING: {
        StringChunkData::deserialize(deSer, *chunkData);
    } break;
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        ListChunkData::deserialize(deSer, *chunkData);
    } break;
    default: {
        // DO NOTHING.
    }
    }

    return chunkData;
}

void BoolChunkData::append(ValueVector* vector, const SelectionVector& selVector) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::BOOL);
    for (auto i = 0u; i < selVector.getSelSize(); i++) {
        const auto pos = selVector[i];
        NullMask::setNull(getData<uint64_t>(), numValues + i, vector->getValue<bool>(pos));
    }
    if (nullData) {
        for (auto i = 0u; i < selVector.getSelSize(); i++) {
            const auto pos = selVector[i];
            nullData->setNull(numValues + i, vector->isNull(pos));
        }
    }
    numValues += selVector.getSelSize();
}

void BoolChunkData::append(ColumnChunkData* other, offset_t startPosInOtherChunk,
    uint32_t numValuesToAppend) {
    NullMask::copyNullMask(other->getData<uint64_t>(), startPosInOtherChunk, getData<uint64_t>(),
        numValues, numValuesToAppend);
    if (nullData) {
        nullData->append(other->getNullData(), startPosInOtherChunk, numValuesToAppend);
    }
    numValues += numValuesToAppend;
}

void BoolChunkData::scan(ValueVector& output, offset_t offset, length_t length,
    sel_t posInOutputVector) const {
    KU_ASSERT(offset + length <= numValues);
    if (nullData) {
        nullData->scan(output, offset, length, posInOutputVector);
    }
    for (auto i = 0u; i < length; i++) {
        output.setValue<bool>(posInOutputVector + i,
            NullMask::isNull(getData<uint64_t>(), offset + i));
    }
}

void BoolChunkData::lookup(offset_t offsetInChunk, ValueVector& output,
    sel_t posInOutputVector) const {
    KU_ASSERT(offsetInChunk < capacity);
    output.setNull(posInOutputVector, nullData->isNull(offsetInChunk));
    if (!output.isNull(posInOutputVector)) {
        output.setValue<bool>(posInOutputVector,
            NullMask::isNull(getData<uint64_t>(), offsetInChunk));
    }
}

void BoolChunkData::write(ColumnChunkData* chunk, ColumnChunkData* dstOffsets, RelMultiplicity) {
    KU_ASSERT(chunk->getDataType().getPhysicalType() == PhysicalTypeID::BOOL &&
              dstOffsets->getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID &&
              chunk->getNumValues() == dstOffsets->getNumValues());
    for (auto i = 0u; i < dstOffsets->getNumValues(); i++) {
        const auto dstOffset = dstOffsets->getValue<offset_t>(i);
        KU_ASSERT(dstOffset < capacity);
        NullMask::setNull(getData<uint64_t>(), dstOffset, chunk->getValue<bool>(i));
        if (nullData) {
            nullData->setNull(dstOffset, chunk->getNullData()->isNull(i));
        }
        numValues = dstOffset >= numValues ? dstOffset + 1 : numValues;
    }
}

void BoolChunkData::write(const ValueVector* vector, offset_t offsetInVector,
    offset_t offsetInChunk) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::BOOL);
    KU_ASSERT(offsetInChunk < capacity);
    setValue(vector->getValue<bool>(offsetInVector), offsetInChunk);
    if (nullData) {
        nullData->write(vector, offsetInVector, offsetInChunk);
    }
    numValues = offsetInChunk >= numValues ? offsetInChunk + 1 : numValues;
}

void BoolChunkData::write(ColumnChunkData* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    if (nullData) {
        nullData->write(srcChunk->getNullData(), srcOffsetInChunk, dstOffsetInChunk,
            numValuesToCopy);
    }
    if ((dstOffsetInChunk + numValuesToCopy) >= numValues) {
        numValues = dstOffsetInChunk + numValuesToCopy;
    }
    NullMask::copyNullMask(srcChunk->getData<uint64_t>(), srcOffsetInChunk, getData<uint64_t>(),
        dstOffsetInChunk, numValuesToCopy);
}

void NullChunkData::setNull(offset_t pos, bool isNull) {
    setValue(isNull, pos);
    if (isNull) {
        mayHaveNullValue = true;
    }
    // TODO(Guodong): Better let NullChunkData also support `append` a
    // vector.
    if (pos >= numValues) {
        numValues = pos + 1;
        KU_ASSERT(numValues <= capacity);
    }
}

void NullChunkData::write(const ValueVector* vector, offset_t offsetInVector,
    offset_t offsetInChunk) {
    setNull(offsetInChunk, vector->isNull(offsetInVector));
    numValues = offsetInChunk >= numValues ? offsetInChunk + 1 : numValues;
}

void NullChunkData::write(ColumnChunkData* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    if ((dstOffsetInChunk + numValuesToCopy) >= numValues) {
        numValues = dstOffsetInChunk + numValuesToCopy;
    }
    copyFromBuffer(srcChunk->getData<uint64_t>(), srcOffsetInChunk, dstOffsetInChunk,
        numValuesToCopy);
}

void NullChunkData::append(ColumnChunkData* other, offset_t startOffsetInOtherChunk,
    uint32_t numValuesToAppend) {
    copyFromBuffer(other->getData<uint64_t>(), startOffsetInOtherChunk, numValues,
        numValuesToAppend);
    numValues += numValuesToAppend;
}

void NullChunkData::serialize(Serializer& serializer) const {
    KU_ASSERT(residencyState == ResidencyState::ON_DISK);
    serializer.writeDebuggingInfo("null_chunk_metadata");
    metadata.serialize(serializer);
}

std::unique_ptr<NullChunkData> NullChunkData::deserialize(MemoryManager& memoryManager,
    Deserializer& deSer) {
    std::string key;
    ColumnChunkMetadata metadata;
    deSer.validateDebuggingInfo(key, "null_chunk_metadata");
    metadata = decltype(metadata)::deserialize(deSer);
    // TODO: FIX-ME. enableCompression.
    return std::make_unique<NullChunkData>(memoryManager, true, metadata);
}

void NullChunkData::scan(ValueVector& output, offset_t offset, length_t length,
    sel_t posInOutputVector) const {
    output.setNullFromBits(getNullMask().getData(), offset, posInOutputVector, length);
}

void InternalIDChunkData::append(ValueVector* vector, const SelectionVector& selVector) {
    switch (vector->dataType.getPhysicalType()) {
    case PhysicalTypeID::INTERNAL_ID: {
        copyVectorToBuffer(vector, numValues, selVector);
    } break;
    case PhysicalTypeID::INT64: {
        copyInt64VectorToBuffer(vector, numValues, selVector);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    numValues += selVector.getSelSize();
}

void InternalIDChunkData::copyVectorToBuffer(ValueVector* vector, offset_t startPosInChunk,
    const SelectionVector& selVector) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
    const auto relIDsInVector = reinterpret_cast<internalID_t*>(vector->getData());
    if (commonTableID == INVALID_TABLE_ID) {
        commonTableID = relIDsInVector[selVector[0]].tableID;
    }
    for (auto i = 0u; i < selVector.getSelSize(); i++) {
        const auto pos = selVector[i];
        if (vector->isNull(pos)) {
            continue;
        }
        KU_ASSERT(relIDsInVector[pos].tableID == commonTableID);
        memcpy(getData() + (startPosInChunk + i) * numBytesPerValue, &relIDsInVector[pos].offset,
            numBytesPerValue);
    }
}

void InternalIDChunkData::copyInt64VectorToBuffer(ValueVector* vector, offset_t startPosInChunk,
    const SelectionVector& selVector) const {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::INT64);
    for (auto i = 0u; i < selVector.getSelSize(); i++) {
        const auto pos = selVector[i];
        if (vector->isNull(pos)) {
            continue;
        }
        memcpy(getData() + (startPosInChunk + i) * numBytesPerValue,
            &vector->getValue<offset_t>(pos), numBytesPerValue);
    }
}

void InternalIDChunkData::scan(ValueVector& output, offset_t offset, length_t length,
    sel_t posInOutputVector) const {
    KU_ASSERT(offset + length <= numValues);
    KU_ASSERT(commonTableID != INVALID_TABLE_ID);
    internalID_t relID;
    relID.tableID = commonTableID;
    for (auto i = 0u; i < length; i++) {
        relID.offset = getValue<offset_t>(offset + i);
        output.setValue<internalID_t>(posInOutputVector + i, relID);
    }
}

void InternalIDChunkData::lookup(offset_t offsetInChunk, ValueVector& output,
    sel_t posInOutputVector) const {
    KU_ASSERT(offsetInChunk < capacity);
    internalID_t relID;
    relID.offset = getValue<offset_t>(offsetInChunk);
    KU_ASSERT(commonTableID != INVALID_TABLE_ID);
    relID.tableID = commonTableID;
    output.setValue<internalID_t>(posInOutputVector, relID);
}

void InternalIDChunkData::write(const ValueVector* vector, offset_t offsetInVector,
    offset_t offsetInChunk) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
    const auto relIDsInVector = reinterpret_cast<internalID_t*>(vector->getData());
    if (commonTableID == INVALID_TABLE_ID) {
        commonTableID = relIDsInVector[offsetInVector].tableID;
    }
    KU_ASSERT(commonTableID == relIDsInVector[offsetInVector].tableID);
    if (!vector->isNull(offsetInVector)) {
        memcpy(getData() + offsetInChunk * numBytesPerValue, &relIDsInVector[offsetInVector].offset,
            numBytesPerValue);
    }
    if (offsetInChunk >= numValues) {
        numValues = offsetInChunk + 1;
    }
}

void InternalIDChunkData::append(ColumnChunkData* other, offset_t startPosInOtherChunk,
    uint32_t numValuesToAppend) {
    ColumnChunkData::append(other, startPosInOtherChunk, numValuesToAppend);
    commonTableID = other->cast<InternalIDChunkData>().commonTableID;
}

std::optional<NullMask> ColumnChunkData::getNullMask() const {
    return nullData ? std::optional(nullData->getNullMask()) : std::nullopt;
}

std::unique_ptr<ColumnChunkData> ColumnChunkFactory::createColumnChunkData(MemoryManager& mm,
    LogicalType dataType, bool enableCompression, uint64_t capacity, ResidencyState residencyState,
    bool hasNullData) {
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        return std::make_unique<BoolChunkData>(mm, capacity, enableCompression, residencyState,
            hasNullData);
    }
    case PhysicalTypeID::INT64:
    case PhysicalTypeID::INT32:
    case PhysicalTypeID::INT16:
    case PhysicalTypeID::INT8:
    case PhysicalTypeID::UINT64:
    case PhysicalTypeID::UINT32:
    case PhysicalTypeID::UINT16:
    case PhysicalTypeID::UINT8:
    case PhysicalTypeID::INT128:
    case PhysicalTypeID::DOUBLE:
    case PhysicalTypeID::FLOAT:
    case PhysicalTypeID::INTERVAL: {
        return std::make_unique<ColumnChunkData>(mm, std::move(dataType), capacity,
            enableCompression, residencyState, hasNullData);
    }
    case PhysicalTypeID::INTERNAL_ID: {
        return std::make_unique<InternalIDChunkData>(mm, capacity, enableCompression,
            residencyState);
    }
    case PhysicalTypeID::STRING: {
        return std::make_unique<StringChunkData>(mm, std::move(dataType), capacity,
            enableCompression, residencyState);
    }
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        return std::make_unique<ListChunkData>(mm, std::move(dataType), capacity, enableCompression,
            residencyState);
    }
    case PhysicalTypeID::STRUCT: {
        return std::make_unique<StructChunkData>(mm, std::move(dataType), capacity,
            enableCompression, residencyState);
    }
    default:
        KU_UNREACHABLE;
    }
}

std::unique_ptr<ColumnChunkData> ColumnChunkFactory::createColumnChunkData(MemoryManager& mm,
    LogicalType dataType, bool enableCompression, ColumnChunkMetadata& metadata, bool hasNullData) {
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        return std::make_unique<BoolChunkData>(mm, enableCompression, metadata, hasNullData);
    }
    case PhysicalTypeID::INT64:
    case PhysicalTypeID::INT32:
    case PhysicalTypeID::INT16:
    case PhysicalTypeID::INT8:
    case PhysicalTypeID::UINT64:
    case PhysicalTypeID::UINT32:
    case PhysicalTypeID::UINT16:
    case PhysicalTypeID::UINT8:
    case PhysicalTypeID::INT128:
    case PhysicalTypeID::DOUBLE:
    case PhysicalTypeID::FLOAT:
    case PhysicalTypeID::INTERVAL: {
        return std::make_unique<ColumnChunkData>(mm, std::move(dataType), enableCompression,
            metadata, hasNullData);
    }
        // Physically, we only materialize offset of INTERNAL_ID, which is same as INT64,
    case PhysicalTypeID::INTERNAL_ID: {
        // INTERNAL_ID should never have nulls.
        return std::make_unique<InternalIDChunkData>(mm, enableCompression, metadata);
    }
    case PhysicalTypeID::STRING: {
        return std::make_unique<StringChunkData>(mm, enableCompression, metadata);
    }
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        return std::make_unique<ListChunkData>(mm, std::move(dataType), enableCompression,
            metadata);
    }
    case PhysicalTypeID::STRUCT: {
        return std::make_unique<StructChunkData>(mm, std::move(dataType), enableCompression,
            metadata);
    }
    default:
        KU_UNREACHABLE;
    }
}

bool ColumnChunkData::isNull(offset_t pos) const {
    return nullData ? nullData->isNull(pos) : false;
}

} // namespace storage
} // namespace kuzu
