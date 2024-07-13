#include "storage/store/column_chunk_data.h"

#include <algorithm>

#include "alp/encode.hpp"
#include "common/data_chunk/sel_vector.h"
#include "common/exception/copy.h"
#include "common/type_utils.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "common/utils.h"
#include "expression_evaluator/expression_evaluator.h"
#include "storage/buffer_manager/bm_file_handle.h"
#include "storage/compression/compression.h"
#include "storage/compression/compression_float.h"
#include "storage/store/list_chunk_data.h"
#include "storage/store/string_chunk_data.h"
#include "storage/store/struct_chunk_data.h"
#include <ranges>

using namespace kuzu::common;
using namespace kuzu::evaluator;

namespace kuzu {
namespace storage {

size_t ColumnChunkMetadata::serializedSizeBytes(common::PhysicalTypeID internalDataType) {
    return sizeof(pageIdx) + sizeof(numPages) + sizeof(numValues) +
           decltype(compMeta)::serializedSizeBytes(internalDataType);
};

std::vector<std::byte> ColumnChunkMetadata::serializeToBytes(
    common::PhysicalTypeID internalDataType) const {
    std::vector<std::byte> ret(serializedSizeBytes(internalDataType));
    offset_t writeOffset = 0;
    writeOffset += writeValueToVector(ret, writeOffset, pageIdx);
    writeOffset += writeValueToVector(ret, writeOffset, numPages);
    writeOffset += writeValueToVector(ret, writeOffset, numValues);

    const auto serializedCompMeta = compMeta.serializeToBytes(internalDataType);
    std::memcpy(ret.data() + writeOffset, serializedCompMeta.data(), serializedCompMeta.size());
    return ret;
}

void ColumnChunkMetadata::deserializeFromBytes(std::span<const std::byte> serializedMetadata,
    common::PhysicalTypeID internalDataType) {
    offset_t readOffset = 0;
    readOffset += readValueFromSpan(serializedMetadata, readOffset, pageIdx);
    readOffset += readValueFromSpan(serializedMetadata, readOffset, numPages);
    readOffset += readValueFromSpan(serializedMetadata, readOffset, numValues);

    compMeta.deserializeFromBytes(serializedMetadata.subspan(readOffset), internalDataType);
}

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

template<std::floating_point T>
class CompressedFloatFlushBuffer {
    std::shared_ptr<FloatCompression<T>> alg;
    const LogicalType& dataType;

public:
    CompressedFloatFlushBuffer(std::shared_ptr<FloatCompression<T>> alg,
        const LogicalType& dataType)
        : alg{std::move(alg)}, dataType{dataType} {}

    CompressedFloatFlushBuffer(const CompressedFloatFlushBuffer& other) = default;

    ColumnChunkMetadata operator()(const uint8_t* buffer, uint64_t bufferSize, BMFileHandle* dataFH,
        page_idx_t startPageIdx, const ColumnChunkMetadata& metadata) const {
        if (metadata.compMeta.compression == CompressionType::UNCOMPRESSED) {
            return CompressedFlushBuffer{std::make_shared<Uncompressed>(dataType), dataType}.
            operator()(buffer, bufferSize, dataFH, startPageIdx, metadata);
        }
        KU_ASSERT(metadata.compMeta.compression == CompressionType::FLOAT);

        auto valuesRemaining = metadata.numValues;
        const uint8_t* bufferStart = buffer;
        const auto compressedBuffer =
            std::make_unique<uint8_t[]>(BufferPoolConstants::PAGE_4KB_SIZE);
        const size_t exceptionBufferSize =
            ceilDiv((size_t)metadata.compMeta.alpMetadata.exceptionCount,
                (BufferPoolConstants::PAGE_4KB_SIZE / EncodeException<T>::sizeBytes())) *
            BufferPoolConstants::PAGE_4KB_SIZE;
        const auto exceptionBuffer = std::make_unique<uint8_t[]>(exceptionBufferSize);
        uint8_t* exceptionBufferCursor = exceptionBuffer.get();
        auto numPages = 0u;
        const auto numValuesPerPage =
            metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
        KU_ASSERT(numValuesPerPage * metadata.numPages >= metadata.numValues);
        size_t remainingExceptionBufferSize = exceptionBufferSize;
        while (valuesRemaining > 0) {
            size_t pageExceptionCount = 0;
            (void)alg->compressNextPageWithExceptions(bufferStart,
                metadata.numValues - valuesRemaining, valuesRemaining, compressedBuffer.get(),
                BufferPoolConstants::PAGE_4KB_SIZE, exceptionBufferCursor,
                remainingExceptionBufferSize, pageExceptionCount, metadata.compMeta);
            exceptionBufferCursor += pageExceptionCount * EncodeException<T>::sizeBytes();
            remainingExceptionBufferSize -= pageExceptionCount * EncodeException<T>::sizeBytes();

            // Avoid underflows (when data is compressed to nothing, numValuesPerPage may be
            // UINT64_MAX)
            if (numValuesPerPage > valuesRemaining) {
                valuesRemaining = 0;
            } else {
                valuesRemaining -= numValuesPerPage;
            }
            KU_ASSERT(numPages < metadata.numPages);
            KU_ASSERT(dataFH->getNumPages() > startPageIdx + numPages);
            dataFH->getFileInfo()->writeFile(compressedBuffer.get(),
                BufferPoolConstants::PAGE_4KB_SIZE,
                (startPageIdx + numPages) * BufferPoolConstants::PAGE_4KB_SIZE);
            numPages++;
        }

        // check for underflow
        KU_ASSERT(remainingExceptionBufferSize <= exceptionBufferSize);

        const auto preExceptionMetadata =
            uncompressedGetMetadata(nullptr, exceptionBufferSize, exceptionBufferSize,
                metadata.compMeta.alpMetadata.exceptionCount, StorageValue{0}, StorageValue{1});

        // TODO doesn't work in multi-thread
        const auto exceptionStartPageIdx = dataFH->addNewPages(preExceptionMetadata.numPages);
        KU_ASSERT(exceptionStartPageIdx == startPageIdx + metadata.numPages);

        if constexpr (std::is_same_v<T, float>) {
            const auto encodedType = LogicalType::ALP_EXCEPTION_FLOAT();
            CompressedFlushBuffer exceptionFlushBuffer{std::make_shared<Uncompressed>(encodedType),
                encodedType};
            const auto exceptionMetadata = exceptionFlushBuffer.operator()(
                reinterpret_cast<const uint8_t*>(exceptionBuffer.get()), exceptionBufferSize,
                dataFH, exceptionStartPageIdx, preExceptionMetadata);

            return ColumnChunkMetadata(startPageIdx, metadata.numPages + exceptionMetadata.numPages,
                metadata.numValues, metadata.compMeta);
        } else {
            const auto encodedType = LogicalType::ALP_EXCEPTION_DOUBLE();
            CompressedFlushBuffer exceptionFlushBuffer{std::make_shared<Uncompressed>(encodedType),
                encodedType};
            const auto exceptionMetadata = exceptionFlushBuffer.operator()(
                reinterpret_cast<const uint8_t*>(exceptionBuffer.get()), exceptionBufferSize,
                dataFH, exceptionStartPageIdx, preExceptionMetadata);

            return ColumnChunkMetadata(startPageIdx, metadata.numPages + exceptionMetadata.numPages,
                metadata.numValues, metadata.compMeta);
        }
    }
};

class GetBitpackingMetadata {
    std::shared_ptr<CompressionAlg> alg;
    const LogicalType& dataType;

public:
    GetBitpackingMetadata(std::shared_ptr<CompressionAlg> alg, const LogicalType& dataType)
        : alg{std::move(alg)}, dataType{dataType} {}

    GetBitpackingMetadata(const GetBitpackingMetadata& other) = default;

    ColumnChunkMetadata operator()(const uint8_t* /*buffer*/, uint64_t /*bufferSize*/,
        uint64_t capacity, uint64_t numValues, StorageValue min, StorageValue max) {
        // For supported types, min and max may be null if all values are null
        // Compression is supported in this case
        // Unsupported types always return a dummy value (where min != max)
        // so that we don't constant compress them
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

template<std::floating_point T>
class GetFloatCompressionMetadata {
    std::shared_ptr<CompressionAlg> alg;
    const LogicalType& dataType;

public:
    GetFloatCompressionMetadata(std::shared_ptr<CompressionAlg> alg, const LogicalType& dataType)
        : alg{std::move(alg)}, dataType{dataType} {}

    GetFloatCompressionMetadata(const GetFloatCompressionMetadata& other) = default;

    ColumnChunkMetadata operator()(const uint8_t* buffer, uint64_t bufferSize, uint64_t capacity,
        uint64_t numValues, StorageValue min, StorageValue max) {
        alp::state alpMetadata;
        if (min == max) {
            return ColumnChunkMetadata(INVALID_PAGE_IDX, 0, numValues,
                CompressionMetadata(min, max, CompressionType::CONSTANT, alpMetadata,
                    StorageValue{0}, StorageValue{0}, 1));
        }

        std::vector<uint8_t> sampleBuffer(bufferSize); // TODO update size
        alp::AlpEncode<T>::init(reinterpret_cast<const T*>(buffer), 0, numValues,
            reinterpret_cast<T*>(sampleBuffer.data()), alpMetadata);
        // TODO avoid compressing if any page goes over the exception limit
        if (alpMetadata.scheme != alp::SCHEME::ALP) {
            return ColumnChunkMetadata(INVALID_PAGE_IDX,
                ColumnChunkData::getNumPagesForBytes(bufferSize), numValues,
                CompressionMetadata(min, max, CompressionType::UNCOMPRESSED));
        }
        if (alpMetadata.k_combinations > 1) {
            alp::AlpEncode<T>::find_best_exponent_factor_from_combinations(
                alpMetadata.best_k_combinations, alpMetadata.k_combinations,
                reinterpret_cast<const T*>(buffer), numValues, alpMetadata.fac, alpMetadata.exp);
        } else {
            KU_ASSERT(alpMetadata.best_k_combinations.size() >= 1);
            alpMetadata.exp = alpMetadata.best_k_combinations[0].first;
            alpMetadata.fac = alpMetadata.best_k_combinations[0].second;
        }

        std::span<const T> src{reinterpret_cast<const T*>(buffer), numValues};
        const auto firstSuccessfulEncode =
            std::find_if(src.begin(), src.end(), [&alpMetadata](T val) {
                const auto encoded_value =
                    alp::AlpEncode<T>::encode_value(val, alpMetadata.fac, alpMetadata.exp);
                const auto decoded_value = alp::AlpDecode<T>::decode_value(encoded_value,
                    alpMetadata.fac, alpMetadata.exp);
                return decoded_value == val;
            });
        if (firstSuccessfulEncode == src.end()) {
            return ColumnChunkMetadata(INVALID_PAGE_IDX,
                ColumnChunkData::getNumPagesForBytes(bufferSize), numValues,
                CompressionMetadata(min, max, CompressionType::UNCOMPRESSED));
        }

        std::vector<int64_t> floatEncodedValues(numValues);
        std::vector<size_t> exceptionsPrefixSum(numValues); // TODO optimize out
        for (offset_t i = 0; i < numValues; ++i) {
            const T& val = src[i];
            const auto encoded_value =
                alp::AlpEncode<T>::encode_value(val, alpMetadata.fac, alpMetadata.exp);
            const auto decoded_value =
                alp::AlpDecode<T>::decode_value(encoded_value, alpMetadata.fac, alpMetadata.exp);

            if (i > 0) {
                exceptionsPrefixSum[i] = exceptionsPrefixSum[i - 1];
            }

            if (val == decoded_value) {
                floatEncodedValues[i] = encoded_value;
            } else {
                floatEncodedValues[i] = *firstSuccessfulEncode;
                ++exceptionsPrefixSum[i];
            }
        }
        const auto& [minEncoded, maxEncoded] =
            std::minmax_element(floatEncodedValues.begin(), floatEncodedValues.end());

        auto compMeta = CompressionMetadata(min, max, alg->getCompressionType(), alpMetadata,
            StorageValue{*minEncoded}, StorageValue{*maxEncoded}, exceptionsPrefixSum.back());

        const auto numValuesPerPage =
            compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
        KU_ASSERT(numValuesPerPage >= 0);
        const auto numPagesForEncoded =
            capacity / numValuesPerPage + (capacity % numValuesPerPage == 0 ? 0 : 1);
        return ColumnChunkMetadata(INVALID_PAGE_IDX, numPagesForEncoded, numValues, compMeta);
    }
};

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
