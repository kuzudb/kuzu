#include "storage/store/column_chunk_data.h"

#include <algorithm>

#include "common/data_chunk/sel_vector.h"
#include "common/exception/copy.h"
#include "common/type_utils.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "expression_evaluator/expression_evaluator.h"
#include "storage/buffer_manager/bm_file_handle.h"
#include "storage/compression/compression.h"
#include "storage/compression/compression_float.h"
#include "storage/store/column_chunk_flush.h"
#include "storage/store/column_chunk_metadata.h"
#include "storage/store/list_chunk_data.h"
#include "storage/store/string_chunk_data.h"
#include "storage/store/struct_chunk_data.h"
#include <ranges>

using namespace kuzu::common;
using namespace kuzu::evaluator;

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

ColumnChunkData::ColumnChunkData(LogicalType dataType, uint64_t capacity, bool enableCompression,
    bool hasNull)
    : dataType{std::move(dataType)}, numBytesPerValue{getDataTypeSizeInChunk(this->dataType)},
      capacity{capacity}, numValues{0}, enableCompression(enableCompression) {
    if (hasNull) {
        nullChunk = std::make_unique<NullChunkData>(capacity, enableCompression);
    }
    initializeBuffer();
    initializeFunction();
}

void ColumnChunkData::initializeBuffer() {
    numBytesPerValue = getDataTypeSizeInChunk(dataType);
    bufferSize = getBufferSize(capacity);
    buffer = std::make_unique<uint8_t[]>(bufferSize);
    if (nullChunk) {
        nullChunk->initializeBuffer();
    }
}

void ColumnChunkData::initializeFunction() {
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        // Since we compress into memory, storage is the same as fixed-sized values,
        // but we need to mark it as being boolean compressed.
        flushBufferFunction = uncompressedFlushBuffer;
        getMetadataFunction = booleanGetMetadata;
    } break;
    case PhysicalTypeID::INT64:
    case PhysicalTypeID::INT32:
    case PhysicalTypeID::INT16:
    case PhysicalTypeID::INT8:
    case PhysicalTypeID::INTERNAL_ID:
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
        const auto compression = std::make_shared<FloatCompression<double>>();
        flushBufferFunction = CompressedFloatFlushBuffer<double>(compression, dataType);
        getMetadataFunction = GetFloatCompressionMetadata<double>(compression, dataType);
        break;
    }
    case PhysicalTypeID::FLOAT: {
        const auto compression = std::make_shared<FloatCompression<float>>();
        flushBufferFunction = CompressedFloatFlushBuffer<float>(compression, dataType);
        getMetadataFunction = GetFloatCompressionMetadata<float>(compression, dataType);
        break;
    }
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST:
    case PhysicalTypeID::STRING:
    default: {
        flushBufferFunction = uncompressedFlushBuffer;
        getMetadataFunction = uncompressedGetMetadata;
    }
    }
}

void ColumnChunkData::resetToAllNull() {
    if (nullChunk) {
        nullChunk->resetToAllNull();
    }
}

void ColumnChunkData::resetToEmpty() {
    if (nullChunk) {
        nullChunk->resetToEmpty();
    }
    KU_ASSERT(bufferSize == getBufferSize(capacity));
    memset(buffer.get(), 0x00, bufferSize);
    numValues = 0;
}
ColumnChunkMetadata ColumnChunkData::getMetadataToFlush() const {
    KU_ASSERT(numValues <= capacity);
    StorageValue minValue{}, maxValue{};
    if (capacity > 0) {
        std::optional<NullMask> nullMask;
        if (nullChunk) {
            nullMask = nullChunk->getNullMask();
        }

        auto [min, max] =
            getMinMaxStorageValue(buffer.get(), 0 /*offset*/, numValues, dataType.getPhysicalType(),
                nullMask.has_value() ? &*nullMask : nullptr, true /*valueRequiredIfUnsupported*/);
        minValue = min.value_or(StorageValue());
        maxValue = max.value_or(StorageValue());
    }
    KU_ASSERT(bufferSize == getBufferSize(capacity));
    return getMetadataFunction(buffer.get(), bufferSize, capacity, numValues, minValue, maxValue);
}

void ColumnChunkData::append(ValueVector* vector, const SelectionVector& selVector) {
    KU_ASSERT(vector->dataType.getPhysicalType() == dataType.getPhysicalType());
    copyVectorToBuffer(vector, numValues, selVector);
    numValues += selVector.getSelSize();
}

void ColumnChunkData::append(ColumnChunkData* other, offset_t startPosInOtherChunk,
    uint32_t numValuesToAppend) {
    KU_ASSERT(other->dataType.getPhysicalType() == dataType.getPhysicalType());
    if (nullChunk) {
        KU_ASSERT(nullChunk->getNumValues() == getNumValues());
        nullChunk->append(other->nullChunk.get(), startPosInOtherChunk, numValuesToAppend);
    }
    KU_ASSERT(numValues + numValuesToAppend <= capacity);
    memcpy(buffer.get() + numValues * numBytesPerValue,
        other->buffer.get() + startPosInOtherChunk * numBytesPerValue,
        numValuesToAppend * numBytesPerValue);
    numValues += numValuesToAppend;
}
ColumnChunkMetadata ColumnChunkData::flushBuffer(BMFileHandle* dataFH, page_idx_t startPageIdx,
    const ColumnChunkMetadata& metadata) const {
    if (!metadata.compMeta.isConstant()) {
        KU_ASSERT(bufferSize == getBufferSize(capacity));
        return flushBufferFunction(buffer.get(), bufferSize, dataFH, startPageIdx, metadata);
    }
    return metadata;
}

uint64_t ColumnChunkData::getBufferSize(uint64_t capacity_) const {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        // 8 values per byte, and we need a buffer size which is a multiple of 8 bytes.
        return ceil(capacity_ / 8.0 / 8.0) * 8;
    }
    default: {
        return numBytesPerValue * capacity_;
    }
    }
}

void ColumnChunkData::lookup(offset_t offsetInChunk, ValueVector& output,
    sel_t posInOutputVector) const {
    KU_ASSERT(offsetInChunk < capacity);
    output.setNull(posInOutputVector, isNull(offsetInChunk));
    if (!output.isNull(posInOutputVector)) {
        memcpy(output.getData() + posInOutputVector * numBytesPerValue,
            buffer.get() + offsetInChunk * numBytesPerValue, numBytesPerValue);
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
        memcpy(buffer.get() + dstOffset * numBytesPerValue, chunk->getData() + i * numBytesPerValue,
            numBytesPerValue);
        numValues = dstOffset >= numValues ? dstOffset + 1 : numValues;
    }
    if (nullChunk || multiplicity == RelMultiplicity::ONE) {
        for (auto i = 0u; i < dstOffsets->getNumValues(); i++) {
            const auto dstOffset = dstOffsets->getValue<offset_t>(i);
            if (multiplicity == RelMultiplicity::ONE && isNull(dstOffset)) {
                throw CopyException(
                    stringFormat("Node with offset: {} can only have one neighbour due "
                                 "to the MANY-ONE/ONE-ONE relationship constraint.",
                        dstOffset));
            }
            if (nullChunk) {
                nullChunk->setNull(dstOffset, chunk->isNull(i));
            }
        }
    }
}

// NOTE: This function is only called in LocalTable right now when performing out-of-place
// committing. LIST has a different logic for handling out-of-place committing as it has to
// be slided. However, this is unsafe, as this function can also be used for other purposes later.
// Thus, an assertion is added at the first line.
void ColumnChunkData::write(ValueVector* vector, offset_t offsetInVector, offset_t offsetInChunk) {
    KU_ASSERT(dataType.getPhysicalType() != PhysicalTypeID::BOOL &&
              dataType.getPhysicalType() != PhysicalTypeID::LIST &&
              dataType.getPhysicalType() != PhysicalTypeID::ARRAY);
    if (nullChunk) {
        nullChunk->setNull(offsetInChunk, vector->isNull(offsetInVector));
    }
    if (offsetInChunk >= numValues) {
        numValues = offsetInChunk + 1;
    }
    if (!vector->isNull(offsetInVector)) {
        memcpy(buffer.get() + offsetInChunk * numBytesPerValue,
            vector->getData() + offsetInVector * numBytesPerValue, numBytesPerValue);
    }
}

void ColumnChunkData::write(ColumnChunkData* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->dataType.getPhysicalType() == dataType.getPhysicalType());
    if ((dstOffsetInChunk + numValuesToCopy) >= numValues) {
        numValues = dstOffsetInChunk + numValuesToCopy;
    }
    memcpy(buffer.get() + dstOffsetInChunk * numBytesPerValue,
        srcChunk->buffer.get() + srcOffsetInChunk * numBytesPerValue,
        numValuesToCopy * numBytesPerValue);
    if (nullChunk) {
        KU_ASSERT(srcChunk->getNullChunk());
        nullChunk->write(srcChunk->getNullChunk(), srcOffsetInChunk, dstOffsetInChunk,
            numValuesToCopy);
    }
}

void ColumnChunkData::copy(ColumnChunkData* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->dataType.getPhysicalType() == dataType.getPhysicalType());
    KU_ASSERT(dstOffsetInChunk >= numValues);
    KU_ASSERT(dstOffsetInChunk < capacity);
    if (nullChunk) {
        while (numValues < dstOffsetInChunk) {
            nullChunk->setNull(numValues, true);
            numValues++;
        }
    } else {
        if (numValues < dstOffsetInChunk) {
            numValues = dstOffsetInChunk;
        }
    }
    append(srcChunk, srcOffsetInChunk, numValuesToCopy);
}

void ColumnChunkData::resize(uint64_t newCapacity) {
    if (newCapacity > capacity) {
        capacity = newCapacity;
    }
    const auto numBytesAfterResize = getBufferSize(newCapacity);
    if (numBytesAfterResize > bufferSize) {
        auto resizedBuffer = std::make_unique<uint8_t[]>(numBytesAfterResize);
        memcpy(resizedBuffer.get(), buffer.get(), bufferSize);
        bufferSize = numBytesAfterResize;
        buffer = std::move(resizedBuffer);
    }
    if (nullChunk) {
        nullChunk->resize(newCapacity);
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
    auto bufferToWrite = buffer.get() + startPosInChunk * numBytesPerValue;
    KU_ASSERT(startPosInChunk + selVector.getSelSize() <= capacity);
    const auto vectorDataToWriteFrom = vector->getData();
    if (selVector.isUnfiltered()) {
        memcpy(bufferToWrite, vectorDataToWriteFrom, selVector.getSelSize() * numBytesPerValue);
        if (nullChunk) {
            // TODO(Guodong): Should be wrapped into nullChunk->append(vector);
            for (auto i = 0u; i < selVector.getSelSize(); i++) {
                nullChunk->setNull(startPosInChunk + i, vector->isNull(i));
            }
        }
    } else {
        for (auto i = 0u; i < selVector.getSelSize(); i++) {
            const auto pos = selVector[i];
            memcpy(bufferToWrite, vectorDataToWriteFrom + pos * numBytesPerValue, numBytesPerValue);
            bufferToWrite += numBytesPerValue;
        }
        if (nullChunk) {
            // TODO(Guodong): Should be wrapped into nullChunk->append(vector);
            for (auto i = 0u; i < selVector.getSelSize(); i++) {
                const auto pos = selVector[i];
                nullChunk->setNull(startPosInChunk + i, vector->isNull(pos));
            }
        }
    }
}

void ColumnChunkData::setNumValues(uint64_t numValues_) {
    KU_ASSERT(numValues_ <= capacity);
    numValues = numValues_;
    if (nullChunk) {
        nullChunk->setNumValues(numValues_);
    }
}

bool ColumnChunkData::numValuesSanityCheck() const {
    if (nullChunk) {
        return numValues == nullChunk->getNumValues();
    }
    return numValues <= capacity;
}

bool ColumnChunkData::sanityCheck() {
    if (nullChunk) {
        return nullChunk->sanityCheck() && numValuesSanityCheck();
    }
    return numValues <= capacity;
}

void BoolChunkData::append(ValueVector* vector, const SelectionVector& selVector) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::BOOL);
    for (auto i = 0u; i < selVector.getSelSize(); i++) {
        const auto pos = selVector[i];
        NullMask::setNull(reinterpret_cast<uint64_t*>(buffer.get()), numValues + i,
            vector->getValue<bool>(pos));
    }
    if (nullChunk) {
        for (auto i = 0u; i < selVector.getSelSize(); i++) {
            const auto pos = selVector[i];
            nullChunk->setNull(numValues + i, vector->isNull(pos));
        }
    }
    numValues += selVector.getSelSize();
}

void BoolChunkData::append(ColumnChunkData* other, offset_t startPosInOtherChunk,
    uint32_t numValuesToAppend) {
    NullMask::copyNullMask(reinterpret_cast<uint64_t*>(other->getData()), startPosInOtherChunk,
        reinterpret_cast<uint64_t*>(buffer.get()), numValues, numValuesToAppend);
    if (nullChunk) {
        nullChunk->append(other->getNullChunk(), startPosInOtherChunk, numValuesToAppend);
    }
    numValues += numValuesToAppend;
}

void BoolChunkData::lookup(offset_t offsetInChunk, ValueVector& output,
    sel_t posInOutputVector) const {
    KU_ASSERT(offsetInChunk < capacity);
    output.setNull(posInOutputVector, nullChunk->isNull(offsetInChunk));
    if (!output.isNull(posInOutputVector)) {
        output.setValue<bool>(posInOutputVector,
            NullMask::isNull(reinterpret_cast<uint64_t*>(buffer.get()), offsetInChunk));
    }
}

void BoolChunkData::write(ColumnChunkData* chunk, ColumnChunkData* dstOffsets, RelMultiplicity) {
    KU_ASSERT(chunk->getDataType().getPhysicalType() == PhysicalTypeID::BOOL &&
              dstOffsets->getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID &&
              chunk->getNumValues() == dstOffsets->getNumValues());
    for (auto i = 0u; i < dstOffsets->getNumValues(); i++) {
        const auto dstOffset = dstOffsets->getValue<offset_t>(i);
        KU_ASSERT(dstOffset < capacity);
        NullMask::setNull(reinterpret_cast<uint64_t*>(buffer.get()), dstOffset,
            chunk->getValue<bool>(i));
        if (nullChunk) {
            nullChunk->setNull(dstOffset, chunk->getNullChunk()->isNull(i));
        }
        numValues = dstOffset >= numValues ? dstOffset + 1 : numValues;
    }
}

void BoolChunkData::write(ValueVector* vector, offset_t offsetInVector, offset_t offsetInChunk) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::BOOL);
    KU_ASSERT(offsetInChunk < capacity);
    setValue(vector->getValue<bool>(offsetInVector), offsetInChunk);
    if (nullChunk) {
        nullChunk->write(vector, offsetInVector, offsetInChunk);
    }
    numValues = offsetInChunk >= numValues ? offsetInChunk + 1 : numValues;
}

void BoolChunkData::write(ColumnChunkData* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    if (nullChunk) {
        nullChunk->write(srcChunk->getNullChunk(), srcOffsetInChunk, dstOffsetInChunk,
            numValuesToCopy);
    }
    if ((dstOffsetInChunk + numValuesToCopy) >= numValues) {
        numValues = dstOffsetInChunk + numValuesToCopy;
    }
    NullMask::copyNullMask(reinterpret_cast<uint64_t*>(srcChunk->getData()), srcOffsetInChunk,
        reinterpret_cast<uint64_t*>(buffer.get()), dstOffsetInChunk, numValuesToCopy);
}

void NullChunkData::setNull(offset_t pos, bool isNull) {
    setValue(isNull, pos);
    if (isNull) {
        mayHaveNullValue = true;
    }
    // TODO(Guodong): Better let NullChunkData also support `append` a vector.
    if (pos >= numValues) {
        numValues = pos + 1;
        KU_ASSERT(numValues <= capacity);
    }
}

void NullChunkData::write(ValueVector* vector, offset_t offsetInVector, offset_t offsetInChunk) {
    setNull(offsetInChunk, vector->isNull(offsetInVector));
    numValues = offsetInChunk >= numValues ? offsetInChunk + 1 : numValues;
}

void NullChunkData::write(ColumnChunkData* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    if ((dstOffsetInChunk + numValuesToCopy) >= numValues) {
        numValues = dstOffsetInChunk + numValuesToCopy;
    }
    copyFromBuffer(reinterpret_cast<uint64_t*>(srcChunk->getData()), srcOffsetInChunk,
        dstOffsetInChunk, numValuesToCopy);
}

void NullChunkData::append(ColumnChunkData* other, offset_t startOffsetInOtherChunk,
    uint32_t numValuesToAppend) {
    copyFromBuffer(reinterpret_cast<uint64_t*>(other->getData()), startOffsetInOtherChunk,
        numValues, numValuesToAppend);
    numValues += numValuesToAppend;
}

class InternalIDChunkData final : public ColumnChunkData {
public:
    // Physically, we only materialize offset of INTERNAL_ID, which is same as UINT64,
    explicit InternalIDChunkData(uint64_t capacity, bool enableCompression)
        : ColumnChunkData(LogicalType::INTERNAL_ID(), capacity, enableCompression,
              false /*hasNull*/),
          commonTableID{INVALID_TABLE_ID} {}

    void append(ValueVector* vector, const SelectionVector& selVector) override {
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

    void copyVectorToBuffer(ValueVector* vector, offset_t startPosInChunk,
        const SelectionVector& selVector) override {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
        const auto relIDsInVector = reinterpret_cast<internalID_t*>(vector->getData());
        if (commonTableID == INVALID_TABLE_ID) {
            commonTableID = relIDsInVector[selVector[0]].tableID;
        }
        for (auto i = 0u; i < selVector.getSelSize(); i++) {
            const auto pos = selVector[i];
            KU_ASSERT(relIDsInVector[pos].tableID == commonTableID);
            memcpy(buffer.get() + (startPosInChunk + i) * numBytesPerValue,
                &relIDsInVector[pos].offset, numBytesPerValue);
        }
    }

    void copyInt64VectorToBuffer(ValueVector* vector, offset_t startPosInChunk,
        const SelectionVector& selVector) const {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::INT64);
        for (auto i = 0u; i < selVector.getSelSize(); i++) {
            const auto pos = selVector[i];
            memcpy(buffer.get() + (startPosInChunk + i) * numBytesPerValue,
                &vector->getValue<offset_t>(pos), numBytesPerValue);
        }
    }

    void lookup(offset_t offsetInChunk, ValueVector& output,
        sel_t posInOutputVector) const override {
        KU_ASSERT(offsetInChunk < capacity);
        output.setNull(posInOutputVector, false);
        if (!output.isNull(posInOutputVector)) {
            auto relID = output.getValue<internalID_t>(posInOutputVector);
            relID.offset = getValue<offset_t>(offsetInChunk);
            KU_ASSERT(commonTableID != INVALID_TABLE_ID);
            relID.tableID = commonTableID;
            output.setValue<internalID_t>(posInOutputVector, relID);
        }
    }

    void write(ValueVector* vector, offset_t offsetInVector, offset_t offsetInChunk) override {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
        const auto relIDsInVector = reinterpret_cast<internalID_t*>(vector->getData());
        if (commonTableID == INVALID_TABLE_ID) {
            commonTableID = relIDsInVector[offsetInVector].tableID;
        }
        KU_ASSERT(commonTableID == relIDsInVector[offsetInVector].tableID);
        if (!vector->isNull(offsetInVector)) {
            memcpy(buffer.get() + offsetInChunk * numBytesPerValue,
                &relIDsInVector[offsetInVector].offset, numBytesPerValue);
        }
        if (offsetInChunk >= numValues) {
            numValues = offsetInChunk + 1;
        }
    }

private:
    table_id_t commonTableID;
};

std::unique_ptr<ColumnChunkData> ColumnChunkFactory::createColumnChunkData(LogicalType dataType,
    bool enableCompression, uint64_t capacity, bool inMemory, bool hasNull) {
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        return std::make_unique<BoolChunkData>(capacity, enableCompression, hasNull);
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
        // TODO: As we have constant compression, SERIAL column should always be compressed as
        // constant non-null when flushed to disk. We should add a sanity check for this.
        return std::make_unique<ColumnChunkData>(std::move(dataType), capacity, enableCompression,
            hasNull);
    }
        // Physically, we only materialize offset of INTERNAL_ID, which is same as INT64,
    case PhysicalTypeID::INTERNAL_ID: {
        return std::make_unique<InternalIDChunkData>(capacity, enableCompression);
    }
    case PhysicalTypeID::STRING: {
        return std::make_unique<StringChunkData>(std::move(dataType), capacity, enableCompression,
            inMemory);
    }
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        return std::make_unique<ListChunkData>(std::move(dataType), capacity, enableCompression,
            inMemory);
    }
    case PhysicalTypeID::STRUCT: {
        return std::make_unique<StructChunkData>(std::move(dataType), capacity, enableCompression,
            inMemory);
    }
    default:
        KU_UNREACHABLE;
    }
}

std::optional<common::NullMask> ColumnChunkData::getNullMask() const {
    return nullChunk ? std::optional(nullChunk->getNullMask()) : std::nullopt;
}

bool ColumnChunkData::isNull(common::offset_t pos) const {
    return nullChunk ? nullChunk->isNull(pos) : false;
}

} // namespace storage
} // namespace kuzu
