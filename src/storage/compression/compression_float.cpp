#include "storage/compression/compression_float.h"

#include "alp/encode.hpp"
#include "common/utils.h"
#include <ranges>

namespace kuzu {
namespace storage {

template<std::floating_point T>
uint64_t FloatCompression<T>::compressNextPage(const uint8_t*& srcBuffer,
    uint64_t numValuesRemaining, uint8_t* dstBuffer, uint64_t dstBufferSize,
    const struct CompressionMetadata& metadata) const {
    KU_UNREACHABLE;
}

template<std::floating_point T>
uint64_t FloatCompression<T>::compressNextPageWithExceptions(const uint8_t*& srcBuffer,
    uint64_t srcOffset, uint64_t numValuesRemaining, uint8_t* dstBuffer, uint64_t dstBufferSize,
    uint8_t* exceptionBuffer, uint64_t exceptionBufferSize, uint64_t& exceptionCount,
    const struct CompressionMetadata& metadata) const {
    // TODO: this is hacky; we need a better system for dynamically choosing between
    // algorithms when compressing
    if (metadata.compression == CompressionType::UNCOMPRESSED) {
        return Uncompressed(sizeof(T)).compressNextPage(srcBuffer, numValuesRemaining, dstBuffer,
            dstBufferSize, metadata);
    }

    const size_t numValuesToCompress =
        std::min(numValuesRemaining, numValues(dstBufferSize, metadata));

    exceptionCount = 0;

    std::vector<EncodedType> integerEncodedValues(numValuesToCompress);
    for (size_t posInPage = 0; posInPage < numValuesToCompress; ++posInPage) {
        const auto floatValue = reinterpret_cast<const T*>(srcBuffer)[posInPage];
        const EncodedType encodedValue = alp::AlpEncode<T>::encode_value(floatValue,
            metadata.alpMetadata.fac, metadata.alpMetadata.exp);
        const double decodedValue = alp::AlpDecode<T>::decode_value(encodedValue,
            metadata.alpMetadata.fac, metadata.alpMetadata.exp);

        if (floatValue != decodedValue) {
            EncodeException<T> exception{.value = floatValue,
                .posInPage = (uint32_t)(srcOffset + posInPage)};
            std::memcpy(exceptionBuffer + exceptionCount * EncodeException<T>::sizeBytes(),
                &exception.value, sizeof(exception.value));
            std::memcpy(exceptionBuffer + exceptionCount * EncodeException<T>::sizeBytes() +
                            sizeof(exception.value),
                &exception.posInPage, sizeof(exception.posInPage));

            // We don't need to replace with 1st successful encode as the integer bitpacking
            // metadata is already populated
            ++exceptionCount;
        } else {
            integerEncodedValues[posInPage] = encodedValue;
        }
    }
    KU_ASSERT(exceptionCount * EncodeException<T>::sizeBytes() <= exceptionBufferSize);
    srcBuffer += numValuesToCompress * sizeof(T);

    const auto* castedIntegerEncodedBuffer =
        reinterpret_cast<const uint8_t*>(integerEncodedValues.data());
    const auto compressedIntegerSize =
        encodedFloatBitpacker.compressNextPage(castedIntegerEncodedBuffer, numValuesToCompress,
            dstBuffer, dstBufferSize, metadata.getChild(0));

    memset(dstBuffer + compressedIntegerSize, 0, dstBufferSize - compressedIntegerSize);

    // since we already do the zeroing we return the size of the whole page
    return dstBufferSize;
}

template<std::floating_point T>
uint64_t FloatCompression<T>::numValues(uint64_t dataSize, const CompressionMetadata& metadata) {
    return decltype(encodedFloatBitpacker)::numValues(dataSize, metadata.getChild(0));
}

template<std::floating_point T>
void FloatCompression<T>::decompressFromPage(const uint8_t* srcBuffer, uint64_t srcOffset,
    uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues,
    const struct CompressionMetadata& metadata) const {

    std::vector<EncodedType> integerEncodedValues(numValues);
    encodedFloatBitpacker.decompressFromPage(srcBuffer, srcOffset,
        reinterpret_cast<uint8_t*>(integerEncodedValues.data()), 0, numValues,
        metadata.getChild(0));

    for (size_t i = 0; i < numValues; ++i) {
        reinterpret_cast<T*>(dstBuffer)[dstOffset + i] = alp::AlpDecode<T>::decode_value(
            integerEncodedValues[i], metadata.alpMetadata.fac, metadata.alpMetadata.exp);
    }
}

template<std::floating_point T>
void FloatCompression<T>::setValuesFromUncompressed(const uint8_t* srcBuffer,
    common::offset_t srcOffset, uint8_t* dstBuffer, common::offset_t dstOffset,
    common::offset_t numValues, const CompressionMetadata& metadata,
    const common::NullMask* nullMask) const {
    KU_UNUSED(nullMask);
    // KU_ASSERT(numValues == static_cast<common::offset_t>(std::ranges::count_if(
    //                            std::ranges::iota_view{srcOffset, srcOffset + numValues},
    //                            [srcBuffer, &metadata, nullMask](common::offset_t i) {
    //                                auto value = reinterpret_cast<const T*>(srcBuffer)[i];
    //                                return (nullMask && nullMask->isNull(i)) ||
    //                                       canUpdateInPlace(std::span(&value, 1), metadata);
    //                            })));

    // TODO fix

    std::vector<EncodedType> integerEncodedValues(numValues);
    for (size_t i = 0; i < numValues; ++i) {
        const size_t posInSrc = i + srcOffset;

        const auto floatValue = reinterpret_cast<const T*>(srcBuffer)[posInSrc];
        const EncodedType encodedValue = alp::AlpEncode<T>::encode_value(floatValue,
            metadata.alpMetadata.fac, metadata.alpMetadata.exp);
        const double decodedValue = alp::AlpDecode<T>::decode_value(encodedValue,
            metadata.alpMetadata.fac, metadata.alpMetadata.exp);
        if (floatValue == decodedValue) {
            integerEncodedValues[i] = encodedValue;
        }
    }

    encodedFloatBitpacker.setValuesFromUncompressed(
        reinterpret_cast<const uint8_t*>(integerEncodedValues.data()), 0, dstBuffer, dstOffset,
        numValues, metadata.getChild(0), nullMask);
}

template<std::floating_point T>
BitpackInfo<int64_t> FloatCompression<T>::getBitpackInfo(const CompressionMetadata& metadata) {
    return decltype(encodedFloatBitpacker)::getPackingInfo(metadata.getChild(0));
}

template<std::floating_point T>
bool FloatCompression<T>::canUpdateInPlace(std::span<const T> value,
    const CompressionMetadata& metadata, const std::optional<common::NullMask>& nullMask,
    uint64_t nullMaskOffset) {
    // TODO implement in-place updates
    return false;
}

template class FloatCompression<double>;
template class FloatCompression<float>;

} // namespace storage
} // namespace kuzu
