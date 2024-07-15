#include "storage/compression/compression_float.h"

#include "alp/encode.hpp"
#include "common/utils.h"
#include <ranges>

namespace kuzu {
namespace storage {

template<std::floating_point T>
ExceptionBuffer<T>::ExceptionBuffer(uint8_t* frame, size_t frameSizeBytes,
    const CompressionMetadata& metadata)
    : capacityBytes(getDataSizeForExceptions(frameSizeBytes, metadata)) {
    uint8_t* exceptionBufferStart = frame + frameSizeBytes;
    header = reinterpret_cast<Header*>(exceptionBufferStart - sizeof(Header));
    exceptions = reinterpret_cast<uint8_t*>(header) - EncodeException<T>::sizeBytes();
}

template<std::floating_point T>
void ExceptionBuffer<T>::init() {
    header->numExceptions = 0;
}

template<std::floating_point T>
void ExceptionBuffer<T>::addException(EncodeException<T> exception) {
    encodeException(exception, exceptionCount());
    ++exceptionCount();
}

template<std::floating_point T>
void ExceptionBuffer<T>::encodeException(EncodeException<T> exception, common::offset_t idx) {
    static constexpr size_t exceptionSizeBytes = exception.sizeBytes();

    const auto byteOffset = idx * exceptionSizeBytes;
    KU_ASSERT(sizeof(Header) + byteOffset + exceptionSizeBytes <= capacityBytes);
    std::memcpy(exceptions - byteOffset, &exception.value, sizeof(exception.value));
    std::memcpy(exceptions - byteOffset + sizeof(exception.value), &exception.posInPage,
        sizeof(exception.posInPage));
}

template<std::floating_point T>
EncodeException<T> ExceptionBuffer<T>::decodeException(common::offset_t idx) {
    EncodeException<T> ret;

    static constexpr size_t exceptionSizeBytes = ret.sizeBytes();

    const auto byteOffset = idx * exceptionSizeBytes;
    KU_ASSERT(sizeof(Header) + byteOffset + exceptionSizeBytes <= capacityBytes);
    std::memcpy(&ret.value, exceptions - byteOffset, sizeof(ret.value));
    std::memcpy(&ret.posInPage, exceptions - byteOffset + sizeof(ret.value), sizeof(ret.posInPage));

    return ret;
}

template<std::floating_point T>
void ExceptionBuffer<T>::removeException(common::offset_t idx) {
    KU_ASSERT(idx < exceptionCount());
    for (size_t i = idx; i < exceptionCount() - 1; ++i) {
        encodeException(decodeException(i + 1), i);
    }
    --exceptionCount();
}

template<std::floating_point T>
size_t ExceptionBuffer<T>::getSizeBytes() {
    return sizeof(Header) + exceptionCount() * EncodeException<T>::sizeBytes();
}

template<std::floating_point T>
common::offset_t& ExceptionBuffer<T>::exceptionCount() {
    KU_ASSERT(header->numExceptions <= 4 * 1024 / EncodeException<T>::sizeBytes());
    return header->numExceptions;
}

template<std::floating_point T>
size_t ExceptionBuffer<T>::getDataSizeForExceptions(size_t dataSize,
    const CompressionMetadata& metadata) {
    return 0;
}

template<std::floating_point T>
size_t ExceptionBuffer<T>::getMaxExceptionCount(size_t exceptionBufferSize) {
    KU_ASSERT(exceptionBufferSize >= sizeof(Header));
    return (exceptionBufferSize - sizeof(Header)) / EncodeException<T>::sizeBytes();
}

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
uint64_t FloatCompression<T>::getMaxExceptionCountPerPage(size_t pageSize,
    const CompressionMetadata& metadata) {
    return ExceptionBuffer<T>::getMaxExceptionCount(
        ExceptionBuffer<T>::getDataSizeForExceptions(pageSize, metadata));
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
    ExceptionBuffer<T> exceptionBuffer{dstBuffer, 4 * 1024, metadata};

    std::vector<EncodedType> integerEncodedValues(numValues);
    size_t successfulEncodeIdx = 0;
    for (size_t i = 0; i < numValues; ++i) {
        const size_t posInPage = i + dstOffset;
        const size_t posInSrc = i + srcOffset;

        const auto floatValue = reinterpret_cast<const T*>(srcBuffer)[posInSrc];
        const EncodedType encodedValue = alp::AlpEncode<T>::encode_value(floatValue,
            metadata.alpMetadata.fac, metadata.alpMetadata.exp);
        const double decodedValue = alp::AlpDecode<T>::decode_value(encodedValue,
            metadata.alpMetadata.fac, metadata.alpMetadata.exp);
        if (floatValue != decodedValue) {
            for (size_t j = 0; j < exceptionBuffer.exceptionCount(); ++j) {
                auto currentException = exceptionBuffer.decodeException(j);
                if (currentException.posInPage == posInPage) {
                    currentException.value = floatValue;
                    exceptionBuffer.removeException(j);
                    exceptionBuffer.addException(currentException);
                    break;
                }
            }
        } else {
            // TODO clean this up
            for (size_t j = 0; j < exceptionBuffer.exceptionCount(); ++j) {
                const auto currentException = exceptionBuffer.decodeException(j);
                if (currentException.posInPage == posInPage) {
                    exceptionBuffer.removeException(j);
                    break;
                }
            }

            integerEncodedValues[successfulEncodeIdx] = encodedValue;
            ++successfulEncodeIdx;
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
