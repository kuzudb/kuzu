#include "storage/store/column_chunk_data.h"

#include <algorithm>

#include "common/data_chunk/sel_vector.h"
#include "common/exception/copy.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/type_utils.h"
#include "common/types/internal_id_t.h"
#include "common/types/interval_t.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "storage/buffer_manager/bm_file_handle.h"
#include "storage/compression/compression.h"
#include "storage/store/list_chunk_data.h"
#include "storage/store/string_chunk_data.h"
#include "storage/store/struct_chunk_data.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {
ColumnChunkMetadata uncompressedFlushBuffer(const uint8_t* buffer, uint64_t bufferSize,
    BMFileHandle* dataFH, page_idx_t startPageIdx, const ColumnChunkMetadata& metadata) {
    KU_ASSERT(dataFH->getNumPages() >= startPageIdx + metadata.numPages);
    dataFH->getFileInfo()->writeFile(buffer, bufferSize,
        startPageIdx * BufferPoolConstants::PAGE_4KB_SIZE);
    return ColumnChunkMetadata(startPageIdx, metadata.numPages, metadata.numValues,
        metadata.compMeta);
}

ColumnChunkMetadata uncompressedGetMetadata(const uint8_t* /*buffer*/, uint64_t bufferSize,
    uint64_t /*capacity*/, uint64_t numValues, StorageValue min, StorageValue max) {
    return ColumnChunkMetadata(INVALID_PAGE_IDX, ColumnChunkData::getNumPagesForBytes(bufferSize),
        numValues, CompressionMetadata(min, max, CompressionType::UNCOMPRESSED));
}

ColumnChunkMetadata booleanGetMetadata(const uint8_t* /*buffer*/, uint64_t bufferSize,
    uint64_t /*capacity*/, uint64_t numValues, StorageValue min, StorageValue max) {
    return ColumnChunkMetadata(INVALID_PAGE_IDX, ColumnChunkData::getNumPagesForBytes(bufferSize),
        numValues, CompressionMetadata(min, max, CompressionType::BOOLEAN_BITPACKING));
}

std::pair<StorageValue, StorageValue> booleanGetMinMax(const uint8_t* buffer, uint64_t numValues) {
    StorageValue minValue{}, maxValue{};
    const auto firstByte = *buffer;
    if (numValues >= 8) {
        if (firstByte == 0b00000000) {
            minValue = false;
            maxValue = false;
        } else if (firstByte == 0b11111111) {
            minValue = true;
            maxValue = true;
        } else {
            minValue = false;
            maxValue = true;
            return {minValue, maxValue};
        }
    } else {
        // First byte will be handled in loop below
        minValue = NullMask::isNull(reinterpret_cast<const uint64_t*>(buffer), 0);
        maxValue = NullMask::isNull(reinterpret_cast<const uint64_t*>(buffer), 0);
    }
    for (size_t i = 0; i < numValues / 8; i++) {
        if (buffer[i] != firstByte) {
            maxValue = true;
            minValue = false;
            return {minValue, maxValue};
        }
    }
    for (size_t i = numValues / 8 * 8; i < numValues; i++) {
        if (minValue.unsignedInt !=
            NullMask::isNull(reinterpret_cast<const uint64_t*>(buffer), i)) {
            minValue = false;
            maxValue = true;
            return {minValue, maxValue};
        }
    }
    return {minValue, maxValue};
}

class CompressedFlushBuffer {
    std::shared_ptr<CompressionAlg> alg;
    const LogicalType& dataType;

public:
    CompressedFlushBuffer(std::shared_ptr<CompressionAlg> alg, const LogicalType& dataType)
        : alg{std::move(alg)}, dataType{dataType} {}

    CompressedFlushBuffer(const CompressedFlushBuffer& other) = default;

    ColumnChunkMetadata operator()(const uint8_t* buffer, uint64_t /*bufferSize*/,
        BMFileHandle* dataFH, page_idx_t startPageIdx, const ColumnChunkMetadata& metadata) const {
        auto valuesRemaining = metadata.numValues;
        const uint8_t* bufferStart = buffer;
        const auto compressedBuffer =
            std::make_unique<uint8_t[]>(BufferPoolConstants::PAGE_4KB_SIZE);
        auto numPages = 0u;
        const auto numValuesPerPage =
            metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
        KU_ASSERT(numValuesPerPage * metadata.numPages >= metadata.numValues);
        while (valuesRemaining > 0) {
            const auto compressedSize = alg->compressNextPage(bufferStart, valuesRemaining,
                compressedBuffer.get(), BufferPoolConstants::PAGE_4KB_SIZE, metadata.compMeta);
            // Avoid underflows (when data is compressed to nothing, numValuesPerPage may be
            // UINT64_MAX)
            if (numValuesPerPage > valuesRemaining) {
                valuesRemaining = 0;
            } else {
                valuesRemaining -= numValuesPerPage;
            }
            if (compressedSize < BufferPoolConstants::PAGE_4KB_SIZE) {
                memset(compressedBuffer.get() + compressedSize, 0,
                    BufferPoolConstants::PAGE_4KB_SIZE - compressedSize);
            }
            KU_ASSERT(numPages < metadata.numPages);
            KU_ASSERT(dataFH->getNumPages() > startPageIdx + numPages);
            dataFH->getFileInfo()->writeFile(compressedBuffer.get(),
                BufferPoolConstants::PAGE_4KB_SIZE,
                (startPageIdx + numPages) * BufferPoolConstants::PAGE_4KB_SIZE);
            numPages++;
        }
        // Make sure that the file is the right length
        if (numPages < metadata.numPages) {
            memset(compressedBuffer.get(), 0, BufferPoolConstants::PAGE_4KB_SIZE);
            dataFH->getFileInfo()->writeFile(compressedBuffer.get(),
                BufferPoolConstants::PAGE_4KB_SIZE,
                (startPageIdx + metadata.numPages - 1) * BufferPoolConstants::PAGE_4KB_SIZE);
        }
        return ColumnChunkMetadata(startPageIdx, metadata.numPages, metadata.numValues,
            metadata.compMeta);
    }
};

class GetCompressionMetadata {
    std::shared_ptr<CompressionAlg> alg;
    const LogicalType& dataType;

public:
    GetCompressionMetadata(std::shared_ptr<CompressionAlg> alg, const LogicalType& dataType)
        : alg{std::move(alg)}, dataType{dataType} {}

    GetCompressionMetadata(const GetCompressionMetadata& other) = default;

    ColumnChunkMetadata operator()(const uint8_t* /*buffer*/, uint64_t /*bufferSize*/,
        uint64_t capacity, uint64_t numValues, StorageValue min, StorageValue max) {
        if (min == max) {
            return ColumnChunkMetadata(INVALID_PAGE_IDX, 0, numValues,
                CompressionMetadata(min, max, CompressionType::CONSTANT));
        }
        auto compMeta = CompressionMetadata(min, max, alg->getCompressionType());
        if (alg->getCompressionType() == CompressionType::INTEGER_BITPACKING) {
            TypeUtils::visit(
                dataType.getPhysicalType(),
                [&]<IntegerBitpackingType T>(T) {
                    // If integer bitpacking bitwidth is the maximum, bitpacking cannot be used
                    // and has poor performance compared to uncompressed
                    if (IntegerBitpacking<T>::getPackingInfo(compMeta).bitWidth >= sizeof(T) * 8) {
                        compMeta = CompressionMetadata(min, max, CompressionType::UNCOMPRESSED);
                    }
                },
                [&](auto) {});
        }
        const auto numValuesPerPage =
            compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
        const auto numPages =
            capacity / numValuesPerPage + (capacity % numValuesPerPage == 0 ? 0 : 1);
        return ColumnChunkMetadata(INVALID_PAGE_IDX, numPages, numValues, compMeta);
    }
};

static std::shared_ptr<CompressionAlg> getCompression(const LogicalType& dataType,
    bool enableCompression) {
    if (!enableCompression) {
        return std::make_shared<Uncompressed>(dataType);
    }
    switch (dataType.getPhysicalType()) {
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
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST:
    case PhysicalTypeID::UINT64: {
        return std::make_shared<IntegerBitpacking<uint64_t>>();
    }
    case PhysicalTypeID::STRING:
    case PhysicalTypeID::UINT32: {
        return std::make_shared<IntegerBitpacking<uint32_t>>();
    }
    case PhysicalTypeID::UINT16: {
        return std::make_shared<IntegerBitpacking<uint16_t>>();
    }
    case PhysicalTypeID::UINT8: {
        return std::make_shared<IntegerBitpacking<uint8_t>>();
    }
    default: {
        return std::make_shared<Uncompressed>(dataType);
    }
    }
}

class GetMinMaxFunction {
    const LogicalType& dataType;

public:
    explicit GetMinMaxFunction(const LogicalType& dataType) : dataType{dataType} {}

    GetMinMaxFunction(const GetMinMaxFunction& other) = default;

    std::pair<StorageValue, StorageValue> operator()(const uint8_t* buffer, uint64_t numValues) {
        KU_ASSERT(dataType.getPhysicalType() != PhysicalTypeID::BOOL);
        StorageValue minValue{}, maxValue{};
        TypeUtils::visit(
            this->dataType.getPhysicalType(),
            [&]<typename T>(T)
                requires(std::integral<T> || std::floating_point<T>)
            {
                const auto& [min, max] = std::minmax_element(reinterpret_cast<const T*>(buffer),
                    reinterpret_cast<const T*>(buffer) + numValues);
                minValue = *min;
                maxValue = *max;
            },
            [&]<typename T>(T)
                requires(std::same_as<T, list_entry_t> || std::same_as<T, internalID_t>)
            {
                const auto& [min, max] =
                    std::minmax_element(reinterpret_cast<const uint64_t*>(buffer),
                        reinterpret_cast<const uint64_t*>(buffer) + numValues);
                minValue = *min;
                maxValue = *max;
            },
            [&](ku_string_t) {
                const auto& [min, max] =
                    std::minmax_element(reinterpret_cast<const uint32_t*>(buffer),
                        reinterpret_cast<const uint32_t*>(buffer) + numValues);
                minValue = *min;
                maxValue = *max;
            },
            // Types which don't currently support statistics
            [&]<typename T>(T)
                requires(std::same_as<T, int128_t> || std::same_as<T, interval_t> ||
                         std::same_as<T, struct_entry_t>)
            {
                minValue = std::numeric_limits<uint64_t>::min();
                maxValue = std::numeric_limits<uint64_t>::max();
            });
        return {minValue, maxValue};
    }
};

ColumnChunkData::ColumnChunkData(LogicalType dataType, uint64_t capacity, bool enableCompression,
    ResidencyState residencyState, bool hasNullData)
    : residencyState{residencyState}, dataType{std::move(dataType)},
      enableCompression{enableCompression},
      numBytesPerValue{getDataTypeSizeInChunk(this->dataType)}, capacity{capacity}, numValues{0} {
    if (hasNullData) {
        nullData = std::make_unique<NullChunkData>(capacity, enableCompression, residencyState);
    }
    initializeBuffer();
    initializeFunction(enableCompression);
}

ColumnChunkData::ColumnChunkData(LogicalType dataType, bool enableCompression,
    const ColumnChunkMetadata& metadata, bool hasNullData)
    : residencyState{ResidencyState::ON_DISK}, dataType{std::move(dataType)},
      enableCompression{enableCompression},
      numBytesPerValue{getDataTypeSizeInChunk(this->dataType)}, bufferSize{0}, capacity{0},
      numValues{metadata.numValues}, metadata{metadata} {
    if (hasNullData) {
        nullData = std::make_unique<NullChunkData>(enableCompression, metadata);
    }
}

void ColumnChunkData::initializeBuffer() {
    numBytesPerValue = getDataTypeSizeInChunk(dataType);
    bufferSize = getBufferSize(capacity);
    buffer = std::make_unique<uint8_t[]>(bufferSize);
    if (nullData) {
        nullData->initializeBuffer();
    }
}

void ColumnChunkData::initializeFunction(bool enableCompression) {
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        // Since we compress into memory, storage is the same as fixed-sized values,
        // but we need to mark it as being boolean compressed.
        flushBufferFunction = uncompressedFlushBuffer;
        getMetadataFunction = booleanGetMetadata;
        getMinMaxFunction = booleanGetMinMax;
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
        getMetadataFunction = GetCompressionMetadata(compression, dataType);
        getMinMaxFunction = GetMinMaxFunction(dataType);
    } break;
    default: {
        flushBufferFunction = uncompressedFlushBuffer;
        getMetadataFunction = uncompressedGetMetadata;
        getMinMaxFunction = GetMinMaxFunction(dataType);
    }
    }
}

void ColumnChunkData::resetToEmpty() {
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
    if (nullData) {
        nullData->resetToEmpty();
    }
    KU_ASSERT(bufferSize == getBufferSize(capacity));
    memset(buffer.get(), 0x00, bufferSize);
    numValues = 0;
}

void ColumnChunkData::setAllNull() {
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
    if (nullData) {
        nullData->setAllNull();
    }
}

ColumnChunkMetadata ColumnChunkData::getMetadataToFlush() const {
    KU_ASSERT(numValues <= capacity);
    StorageValue minValue = {}, maxValue = {};
    if (capacity > 0) {
        auto [min, max] = getMinMaxFunction(buffer.get(), numValues);
        minValue = min;
        maxValue = max;
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
    if (nullData) {
        KU_ASSERT(nullData->getNumValues() == getNumValues());
        nullData->append(other->nullData.get(), startPosInOtherChunk, numValuesToAppend);
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

void ColumnChunkData::initializeScanState(ChunkState& state) const {
    if (nullData) {
        nullData->initializeScanState(*state.nullState);
    }
    state.metadata = metadata;
    state.numValuesPerPage =
        state.metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
}

void ColumnChunkData::scan(ValueVector& output, offset_t offset, length_t length) const {
    KU_ASSERT(offset + length <= numValues);
    // TODO(Guodong): Rework properly as scan and handle visibility here.
    for (auto i = 0u; i < length; i++) {
        lookup(offset + i, output, i);
    }
}

void ColumnChunkData::lookup(offset_t offsetInChunk, ValueVector& output,
    sel_t posInOutputVector) const {
    KU_ASSERT(offsetInChunk < capacity);
    // TODO(Guodong): Handle visibility here.
    output.setNull(posInOutputVector, nullData->isNull(offsetInChunk));
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
        if (multiplicity == RelMultiplicity::ONE && !nullData->isNull(dstOffset)) {
            throw CopyException(stringFormat("Node with offset: {} can only have one neighbour due "
                                             "to the MANY-ONE/ONE-ONE relationship constraint.",
                dstOffset));
        }
        if (!chunk->getNullData()->isNull(i)) {
            memcpy(buffer.get() + dstOffset * numBytesPerValue,
                chunk->getData() + i * numBytesPerValue, numBytesPerValue);
        }
        nullData->setNull(dstOffset, chunk->getNullData()->isNull(i));
        numValues = dstOffset >= numValues ? dstOffset + 1 : numValues;
    }
}

// NOTE: This function is only called in LocalTable right now when performing out-of-place
// committing. LIST has a different logic for handling out-of-place committing as it has to
// be slided. However, this is unsafe, as this function can also be used for other purposes later.
// Thus, an assertion is added at the first line.
void ColumnChunkData::write(const ValueVector* vector, offset_t offsetInVector,
    offset_t offsetInChunk) {
    KU_ASSERT(dataType.getPhysicalType() != PhysicalTypeID::BOOL &&
              dataType.getPhysicalType() != PhysicalTypeID::LIST &&
              dataType.getPhysicalType() != PhysicalTypeID::ARRAY);
    nullData->setNull(offsetInChunk, vector->isNull(offsetInVector));
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
    nullData->write(srcChunk->getNullData(), srcOffsetInChunk, dstOffsetInChunk, numValuesToCopy);
}

void ColumnChunkData::copy(ColumnChunkData* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->dataType.getPhysicalType() == dataType.getPhysicalType());
    KU_ASSERT(dstOffsetInChunk >= numValues);
    while (numValues < dstOffsetInChunk) {
        nullData->setNull(numValues, true);
        numValues++;
    }
    append(srcChunk, srcOffsetInChunk, numValuesToCopy);
}

void ColumnChunkData::resize(uint64_t newCapacity) {
    KU_ASSERT(
        residencyState == ResidencyState::IN_MEMORY || residencyState == ResidencyState::TEMPORARY);
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
    if (nullData) {
        nullData->resize(newCapacity);
    }
}

void ColumnChunkData::populateWithDefaultVal(ValueVector* defaultValueVector) {
    // TODO(Guodong): don't set vector state to a new one. Default vector is shared across all
    // operators on the pipeline so setting its state will affect others.
    // You can only set state for vectors that is local to this class.
    defaultValueVector->setState(std::make_shared<DataChunkState>());
    const auto valPos = defaultValueVector->state->getSelVector()[0];
    auto positionBuffer = defaultValueVector->state->getSelVectorUnsafe().getMultableBuffer();
    for (auto i = 0u; i < defaultValueVector->state->getSelVector().getSelSize(); i++) {
        positionBuffer[i] = valPos;
    }
    defaultValueVector->state->getSelVectorUnsafe().setToFiltered(DEFAULT_VECTOR_CAPACITY);
    auto numValuesAppended = 0u;
    const auto numValuesToPopulate = capacity;
    while (numValuesAppended < numValuesToPopulate) {
        const auto numValuesToAppend =
            std::min(DEFAULT_VECTOR_CAPACITY, numValuesToPopulate - numValuesAppended);
        defaultValueVector->state->getSelVectorUnsafe().setSelSize(numValuesToAppend);
        append(defaultValueVector, defaultValueVector->state->getSelVector());
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
        // TODO(Guodong): Should be wrapped into nullChunk->append(vector);
        for (auto i = 0u; i < selVector.getSelSize(); i++) {
            nullData->setNull(startPosInChunk + i, vector->isNull(i));
        }
    } else {
        for (auto i = 0u; i < selVector.getSelSize(); i++) {
            const auto pos = selVector[i];
            // TODO(Guodong): Should be wrapped into nullChunk->append(vector);
            nullData->setNull(startPosInChunk + i, vector->isNull(pos));
            memcpy(bufferToWrite, vectorDataToWriteFrom + pos * numBytesPerValue, numBytesPerValue);
            bufferToWrite += numBytesPerValue;
        }
    }
}

void ColumnChunkData::setNumValues(uint64_t numValues_) {
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
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
    return bufferSize + (nullData ? nullData->getEstimatedMemoryUsage() : 0);
}

void ColumnChunkData::serialize(Serializer& serializer) const {
    KU_ASSERT(residencyState == ResidencyState::ON_DISK);
    dataType.serialize(serializer);
    serializer.write<ColumnChunkMetadata>(metadata);
    serializer.write<bool>(nullData != nullptr);
    if (nullData) {
        nullData->serialize(serializer);
    }
}

std::unique_ptr<ColumnChunkData> ColumnChunkData::deserialize(Deserializer& deSer) {
    const auto dataType = LogicalType::deserialize(deSer);
    ColumnChunkMetadata metadata;
    deSer.deserializeValue<ColumnChunkMetadata>(metadata);
    bool hasNull = false;
    deSer.deserializeValue<bool>(hasNull);
    KU_ASSERT(hasNull);
    // TODO(Guodong): FIX-ME. enableCompression.
    auto chunkData = ColumnChunkFactory::createColumnChunkData(*dataType, true, metadata);
    chunkData->nullData = NullChunkData::deserialize(deSer);

    switch (dataType->getPhysicalType()) {
    case PhysicalTypeID::STRUCT: {
        StructChunkData::deserialize(deSer, *chunkData);
    } break;
    case PhysicalTypeID::STRING: {
        StringChunkData::deserialize(deSer, *chunkData);
    } break;
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
        nullData->setNull(numValues + i, vector->isNull(pos));
        NullMask::setNull(reinterpret_cast<uint64_t*>(buffer.get()), numValues + i,
            vector->getValue<bool>(pos));
    }
    numValues += selVector.getSelSize();
}

void BoolChunkData::append(ColumnChunkData* other, offset_t startPosInOtherChunk,
    uint32_t numValuesToAppend) {
    NullMask::copyNullMask(reinterpret_cast<uint64_t*>(other->getData()), startPosInOtherChunk,
        reinterpret_cast<uint64_t*>(buffer.get()), numValues, numValuesToAppend);
    if (nullData) {
        nullData->append(other->getNullData(), startPosInOtherChunk, numValuesToAppend);
    }
    numValues += numValuesToAppend;
}

void BoolChunkData::lookup(offset_t offsetInChunk, ValueVector& output,
    sel_t posInOutputVector) const {
    KU_ASSERT(offsetInChunk < capacity);
    output.setNull(posInOutputVector, nullData->isNull(offsetInChunk));
    if (!output.isNull(posInOutputVector)) {
        output.setValue<bool>(posInOutputVector,
            NullMask::isNull(reinterpret_cast<uint64_t*>(buffer.get()), offsetInChunk));
    }
}

void BoolChunkData::write(ColumnChunkData* chunk, ColumnChunkData* dstOffsets,
    RelMultiplicity /*multiplicity*/) {
    KU_ASSERT(chunk->getDataType().getPhysicalType() == PhysicalTypeID::BOOL &&
              dstOffsets->getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID &&
              chunk->getNumValues() == dstOffsets->getNumValues());
    for (auto i = 0u; i < dstOffsets->getNumValues(); i++) {
        const auto dstOffset = dstOffsets->getValue<offset_t>(i);
        KU_ASSERT(dstOffset < capacity);
        NullMask::setNull(reinterpret_cast<uint64_t*>(buffer.get()), dstOffset,
            chunk->getValue<bool>(i));
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
    copyFromBuffer(reinterpret_cast<uint64_t*>(srcChunk->getData()), srcOffsetInChunk,
        dstOffsetInChunk, numValuesToCopy);
}

void NullChunkData::append(ColumnChunkData* other, offset_t startOffsetInOtherChunk,
    uint32_t numValuesToAppend) {
    copyFromBuffer(reinterpret_cast<uint64_t*>(other->getData()), startOffsetInOtherChunk,
        numValues, numValuesToAppend);
    numValues += numValuesToAppend;
}

void NullChunkData::serialize(Serializer& serializer) const {
    KU_ASSERT(residencyState == ResidencyState::ON_DISK);
    serializer.write<ColumnChunkMetadata>(metadata);
}

std::unique_ptr<NullChunkData> NullChunkData::deserialize(Deserializer& deSer) {
    ColumnChunkMetadata metadata;
    deSer.deserializeValue<ColumnChunkMetadata>(metadata);
    // TODO: FIX-ME. enableCompression.
    return std::make_unique<NullChunkData>(true, metadata);
}

class InternalIDChunkData final : public ColumnChunkData {
public:
    // Physically, we only materialize offset of INTERNAL_ID, which is same as UINT64,
    InternalIDChunkData(uint64_t capacity, bool enableCompression, ResidencyState residencyState)
        : ColumnChunkData(*LogicalType::INTERNAL_ID(), capacity, enableCompression, residencyState,
              false /*hasNullData*/),
          commonTableID{INVALID_TABLE_ID} {}
    InternalIDChunkData(bool enableCompression, const ColumnChunkMetadata& metadata)
        : ColumnChunkData{*LogicalType::INTERNAL_ID(), enableCompression, metadata,
              false /*hasNullData*/},
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
            nullData->setNull(startPosInChunk + i, vector->isNull(pos));
            memcpy(buffer.get() + (startPosInChunk + i) * numBytesPerValue,
                &relIDsInVector[pos].offset, numBytesPerValue);
        }
    }

    void copyInt64VectorToBuffer(ValueVector* vector, offset_t startPosInChunk,
        const SelectionVector& selVector) const {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::INT64);
        for (auto i = 0u; i < selVector.getSelSize(); i++) {
            const auto pos = selVector[i];
            nullData->setNull(startPosInChunk + i, vector->isNull(pos));
            memcpy(buffer.get() + (startPosInChunk + i) * numBytesPerValue,
                &vector->getValue<offset_t>(pos), numBytesPerValue);
        }
    }

    void lookup(offset_t offsetInChunk, ValueVector& output,
        sel_t posInOutputVector) const override {
        KU_ASSERT(offsetInChunk < capacity);
        output.setNull(posInOutputVector, nullData->isNull(offsetInChunk));
        if (!output.isNull(posInOutputVector)) {
            auto relID = output.getValue<internalID_t>(posInOutputVector);
            relID.offset = getValue<offset_t>(offsetInChunk);
            KU_ASSERT(commonTableID != INVALID_TABLE_ID);
            relID.tableID = commonTableID;
            output.setValue<internalID_t>(posInOutputVector, relID);
        }
    }

    void write(const ValueVector* vector, offset_t offsetInVector,
        offset_t offsetInChunk) override {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
        nullData->setNull(offsetInChunk, vector->isNull(offsetInVector));
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
    bool enableCompression, uint64_t capacity, ResidencyState residencyState, bool hasNullData) {
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        return std::make_unique<BoolChunkData>(capacity, enableCompression, residencyState,
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
        return std::make_unique<ColumnChunkData>(std::move(dataType), capacity, enableCompression,
            residencyState, hasNullData);
    }
        // Physically, we only materialize offset of INTERNAL_ID, which is same as INT64,
    case PhysicalTypeID::INTERNAL_ID: {
        // INTERNAL_ID should never have nulls.
        return std::make_unique<InternalIDChunkData>(capacity, enableCompression, residencyState);
    }
    case PhysicalTypeID::STRING: {
        return std::make_unique<StringChunkData>(std::move(dataType), capacity, enableCompression,
            residencyState);
    }
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        return std::make_unique<ListChunkData>(std::move(dataType), capacity, enableCompression,
            residencyState);
    }
    case PhysicalTypeID::STRUCT: {
        return std::make_unique<StructChunkData>(std::move(dataType), capacity, enableCompression,
            residencyState);
    }
    default:
        KU_UNREACHABLE;
    }
}

std::unique_ptr<ColumnChunkData> ColumnChunkFactory::createColumnChunkData(LogicalType dataType,
    bool enableCompression, ColumnChunkMetadata& metadata) {
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        return std::make_unique<BoolChunkData>(enableCompression, metadata, true /*hasNullData*/);
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
        return std::make_unique<ColumnChunkData>(std::move(dataType), enableCompression, metadata,
            true /*hasNullData*/);
    }
        // Physically, we only materialize offset of INTERNAL_ID, which is same as INT64,
    case PhysicalTypeID::INTERNAL_ID: {
        // INTERNAL_ID should never have nulls.
        return std::make_unique<InternalIDChunkData>(enableCompression, metadata);
    }
    case PhysicalTypeID::STRING: {
        return std::make_unique<StringChunkData>(enableCompression, metadata);
    }
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        return std::make_unique<ListChunkData>(std::move(dataType), enableCompression, metadata);
    }
    case PhysicalTypeID::STRUCT: {
        return std::make_unique<StructChunkData>(std::move(dataType), enableCompression, metadata);
    }
    default:
        KU_UNREACHABLE;
    }
}

} // namespace storage
} // namespace kuzu
