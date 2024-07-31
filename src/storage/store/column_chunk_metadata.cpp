#include "storage/store/column_chunk_metadata.h"

#include "alp/decode.hpp"
#include "alp/encode.hpp"
#include "common/constants.h"
#include "common/type_utils.h"
#include "common/utils.h"
#include "storage/compression/compression_float.h"
#include "storage/store/column_chunk_data.h"

namespace kuzu::storage {
using namespace common;

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

ColumnChunkMetadata GetBitpackingMetadata::operator()(const uint8_t* /*buffer*/,
    uint64_t /*bufferSize*/, uint64_t capacity, uint64_t numValues, StorageValue min,
    StorageValue max) {
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
    const auto numValuesPerPage = compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
    const auto numPages =
        numValuesPerPage == UINT64_MAX ?
            0 :
            capacity / numValuesPerPage + (capacity % numValuesPerPage == 0 ? 0 : 1);
    return ColumnChunkMetadata(INVALID_PAGE_IDX, numPages, numValues, compMeta);
}

template<std::floating_point T>
ColumnChunkMetadata GetFloatCompressionMetadata<T>::operator()(const uint8_t* buffer,
    uint64_t bufferSize, uint64_t capacity, uint64_t numValues, StorageValue min,
    StorageValue max) {
    const PhysicalTypeID physicalType =
        std::same_as<T, double> ? PhysicalTypeID::DOUBLE : PhysicalTypeID::FLOAT;

    alp::state alpMetadata;
    if (min == max) {
        return ColumnChunkMetadata(INVALID_PAGE_IDX, 0, numValues,
            CompressionMetadata(min, max, CompressionType::CONSTANT, alpMetadata, StorageValue{0},
                StorageValue{0}, physicalType));
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
    const auto firstSuccessfulEncode = std::find_if(src.begin(), src.end(), [&alpMetadata](T val) {
        const auto encoded_value =
            alp::AlpEncode<T>::encode_value(val, alpMetadata.fac, alpMetadata.exp);
        const auto decoded_value =
            alp::AlpDecode<T>::decode_value(encoded_value, alpMetadata.fac, alpMetadata.exp);
        return decoded_value == val;
    });
    if (firstSuccessfulEncode == src.end()) {
        return ColumnChunkMetadata(INVALID_PAGE_IDX,
            ColumnChunkData::getNumPagesForBytes(bufferSize), numValues,
            CompressionMetadata(min, max, CompressionType::UNCOMPRESSED));
    }

    std::vector<int64_t> floatEncodedValues(numValues);
    size_t exceptionCount = 0;
    for (offset_t i = 0; i < numValues; ++i) {
        const T& val = src[i];
        const auto encoded_value =
            alp::AlpEncode<T>::encode_value(val, alpMetadata.fac, alpMetadata.exp);
        const auto decoded_value =
            alp::AlpDecode<T>::decode_value(encoded_value, alpMetadata.fac, alpMetadata.exp);

        if (val == decoded_value) {
            floatEncodedValues[i] = encoded_value;
        } else {
            floatEncodedValues[i] = *firstSuccessfulEncode;
            ++exceptionCount;
        }
    }

    const auto& [minEncoded, maxEncoded] =
        std::minmax_element(floatEncodedValues.begin(), floatEncodedValues.end());

    alpMetadata.exceptions_count = exceptionCount;
    auto compMeta = CompressionMetadata(min, max, alg->getCompressionType(), alpMetadata,
        StorageValue{*minEncoded}, StorageValue{*maxEncoded}, physicalType);

    const auto bitpackingInfo = FloatCompression<T>::getBitpackInfo(compMeta);
    if (ceilDiv(bitpackingInfo.bitWidth * (numValues - exceptionCount), static_cast<uint64_t>(8)) +
            EncodeException<T>::sizeBytes() * exceptionCount >=
        numValues * sizeof(T)) {
        return ColumnChunkMetadata(INVALID_PAGE_IDX,
            ColumnChunkData::getNumPagesForBytes(bufferSize), numValues,
            CompressionMetadata(min, max, CompressionType::UNCOMPRESSED));
    }

    const auto numValuesPerPage = compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
    KU_ASSERT(numValuesPerPage >= 0);
    const auto numPagesForEncoded =
        capacity / numValuesPerPage + (capacity % numValuesPerPage == 0 ? 0 : 1);
    // TODO: consolidate
    const auto exceptionCapacity = std::bit_ceil(exceptionCount * sizeof(T)) / sizeof(T);
    const auto numPagesForExceptions = ceilDiv(static_cast<uint64_t>(exceptionCapacity),
        (BufferPoolConstants::PAGE_4KB_SIZE / EncodeException<T>::sizeBytes()));
    return ColumnChunkMetadata(INVALID_PAGE_IDX, numPagesForEncoded + numPagesForExceptions,
        numValues, compMeta);
}

template class GetFloatCompressionMetadata<float>;
template class GetFloatCompressionMetadata<double>;
} // namespace kuzu::storage
