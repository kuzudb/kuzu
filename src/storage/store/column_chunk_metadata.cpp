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

namespace {

ColumnChunkMetadata uncompressedGetMetadataInternal(uint64_t bufferSize, uint64_t numValues,
    StorageValue min, StorageValue max) {
    return ColumnChunkMetadata(INVALID_PAGE_IDX, ColumnChunkData::getNumPagesForBytes(bufferSize),
        numValues, CompressionMetadata(min, max, CompressionType::UNCOMPRESSED));
}
} // namespace

ColumnChunkMetadata uncompressedGetMetadata(const uint8_t* /*buffer*/, uint64_t bufferSize,
    uint64_t /*capacity*/, uint64_t numValues, StorageValue min, StorageValue max) {
    return uncompressedGetMetadataInternal(bufferSize, numValues, min, max);
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

namespace {
ColumnChunkMetadata getConstantFloatMetadata(PhysicalTypeID physicalType, uint64_t numValues,
    StorageValue min, StorageValue max) {
    return {INVALID_PAGE_IDX, 0, numValues,
        CompressionMetadata(min, max, CompressionType::CONSTANT, alp::state{}, StorageValue{0},
            StorageValue{0}, physicalType)};
}

template<std::floating_point T>
alp::state getAlpMetadata(const uint8_t* buffer, uint64_t numValues) {
    alp::state alpMetadata;
    std::vector<uint8_t> sampleBuffer(alp::config::SAMPLES_PER_ROWGROUP);
    alp::AlpEncode<T>::init(reinterpret_cast<const T*>(buffer), 0, numValues,
        reinterpret_cast<T*>(sampleBuffer.data()), alpMetadata);

    if (alpMetadata.scheme == alp::SCHEME::ALP) {
        if (alpMetadata.k_combinations > 1) {
            alp::AlpEncode<T>::find_best_exponent_factor_from_combinations(
                alpMetadata.best_k_combinations, alpMetadata.k_combinations,
                reinterpret_cast<const T*>(buffer), numValues, alpMetadata.fac, alpMetadata.exp);
        } else {
            KU_ASSERT(alpMetadata.best_k_combinations.size() >= 1);
            alpMetadata.exp = alpMetadata.best_k_combinations[0].first;
            alpMetadata.fac = alpMetadata.best_k_combinations[0].second;
        }
    }

    return alpMetadata;
}

template<std::floating_point T>
std::optional<T> getFirstSuccessfulALPEncode(std::span<const T> src,
    const alp::state& alpMetadata) {
    const auto firstSuccessfulEncode = std::find_if(src.begin(), src.end(), [&alpMetadata](T val) {
        const auto encoded_value =
            alp::AlpEncode<T>::encode_value(val, alpMetadata.fac, alpMetadata.exp);
        const auto decoded_value =
            alp::AlpDecode<T>::decode_value(encoded_value, alpMetadata.fac, alpMetadata.exp);
        return decoded_value == val;
    });
    if (firstSuccessfulEncode == src.end()) {
        return {};
    }
    return alp::AlpEncode<T>::encode_value(*firstSuccessfulEncode, alpMetadata.fac,
        alpMetadata.exp);
}

template<std::floating_point T>
CompressionMetadata getFloatBitpackingMetadata(CompressionType compressionType,
    PhysicalTypeID physicalType, std::span<const T> src, T firstSuccessfulEncode,
    alp::state& alpMetadata, StorageValue min, StorageValue max) {
    std::vector<typename FloatCompression<T>::EncodedType> floatEncodedValues(src.size());
    size_t exceptionCount = 0;
    for (offset_t i = 0; i < src.size(); ++i) {
        const T& val = src[i];
        const auto encoded_value =
            alp::AlpEncode<T>::encode_value(val, alpMetadata.fac, alpMetadata.exp);
        const auto decoded_value =
            alp::AlpDecode<T>::decode_value(encoded_value, alpMetadata.fac, alpMetadata.exp);

        if (val == decoded_value) {
            floatEncodedValues[i] = encoded_value;
        } else {
            floatEncodedValues[i] = firstSuccessfulEncode;
            ++exceptionCount;
        }
    }
    alpMetadata.exceptions_count = exceptionCount;

    const auto& [minEncoded, maxEncoded] =
        std::minmax_element(floatEncodedValues.begin(), floatEncodedValues.end());

    return CompressionMetadata(min, max, compressionType, alpMetadata, StorageValue{*minEncoded},
        StorageValue{*maxEncoded}, physicalType);
}
} // namespace

template<std::floating_point T>
ColumnChunkMetadata GetFloatCompressionMetadata<T>::operator()(const uint8_t* buffer,
    uint64_t bufferSize, uint64_t capacity, uint64_t numValues, StorageValue min,
    StorageValue max) {
    const PhysicalTypeID physicalType =
        std::same_as<T, double> ? PhysicalTypeID::DOUBLE : PhysicalTypeID::FLOAT;

    if (min == max) {
        return getConstantFloatMetadata(physicalType, numValues, min, max);
    }

    if (numValues == 0) {
        return uncompressedGetMetadataInternal(bufferSize, numValues, min, max);
    }

    alp::state alpMetadata = getAlpMetadata<T>(buffer, numValues);
    if (alpMetadata.scheme != alp::SCHEME::ALP) {
        return uncompressedGetMetadataInternal(bufferSize, numValues, min, max);
    }

    std::span<const T> castedBuffer{reinterpret_cast<const T*>(buffer), numValues};
    const auto firstSuccessfulEncode = getFirstSuccessfulALPEncode<T>(castedBuffer, alpMetadata);
    if (!firstSuccessfulEncode.has_value()) {
        return uncompressedGetMetadataInternal(bufferSize, numValues, min, max);
    }

    const auto compMeta = getFloatBitpackingMetadata(alg->getCompressionType(), physicalType,
        castedBuffer, firstSuccessfulEncode.value(), alpMetadata, min, max);
    const auto exceptionCount = compMeta.floatMetadata().exceptionCount;

    static constexpr size_t MAX_EXCEPTION_FACTOR = 4;
    if (exceptionCount * MAX_EXCEPTION_FACTOR >= numValues) {
        return uncompressedGetMetadataInternal(bufferSize, numValues, min, max);
    }

    const auto numValuesPerPage = compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
    const auto numPagesForEncoded = ceilDiv(capacity, numValuesPerPage);
    const auto numPagesForExceptions =
        EncodeException<T>::numPagesFromExceptions(compMeta.floatMetadata().exceptionCapacity);
    return ColumnChunkMetadata(INVALID_PAGE_IDX, numPagesForEncoded + numPagesForExceptions,
        numValues, compMeta);
}

template class GetFloatCompressionMetadata<float>;
template class GetFloatCompressionMetadata<double>;
} // namespace kuzu::storage
