#include "storage/compression/compression_float.h"

#include "alp/encode.hpp"
#include "common/utils.h"
#include <ranges>

namespace kuzu {
namespace storage {
namespace {

template<std::floating_point T>
struct ExceptionBuffer {
    struct EncodeException {
        T value;
        uint16_t posInPage; // TODO maybe make this a smaller type to store more exceptions

        static constexpr size_t sizeBytes();
    };

    ExceptionBuffer(uint8_t* frame, size_t frameSizeBytes, const CompressionMetadata& metadata);
    struct Header {
        common::offset_t numExceptions;
    }* header;
    uint8_t* exceptions;
    size_t capacityBytes;

    void init();

    void addException(EncodeException exception);
    void encodeException(EncodeException exception, common::offset_t idx);
    EncodeException decodeException(common::offset_t idx);
    void removeException(common::offset_t idx);
    size_t getSizeBytes();
    common::offset_t& exceptionCount();

    // at most 1 / MAX_EXCEPTION_FACTOR * totalCompressedValues can be exceptions
    // if this is not the case we avoid compressing altogether
    static constexpr size_t MAX_EXCEPTION_FACTOR = 8;

    static size_t getDataSizeForExceptions(size_t dataSize, const CompressionMetadata& metadata);
    static size_t getMaxExceptionCount(size_t exceptionBufferSize);
};

template<std::floating_point T>
constexpr size_t ExceptionBuffer<T>::EncodeException::sizeBytes() {
    constexpr size_t exceptionSizeBytes = sizeof(value) + sizeof(posInPage);

    // best we can do to ensure that all fields are accounted for due to struct padding
    static_assert(exceptionSizeBytes + (8 - exceptionSizeBytes % 8) >= sizeof(EncodeException));

    return exceptionSizeBytes;
}

template<std::floating_point T>
ExceptionBuffer<T>::ExceptionBuffer(uint8_t* frame, size_t frameSizeBytes,
    const CompressionMetadata& metadata)
    : capacityBytes(getDataSizeForExceptions(frameSizeBytes, metadata)) {
    uint8_t* exceptionBufferStart = frame + frameSizeBytes - capacityBytes;
    header = reinterpret_cast<Header*>(exceptionBufferStart);
    exceptions = exceptionBufferStart + sizeof(Header);
}

template<std::floating_point T>
void ExceptionBuffer<T>::init() {
    header->numExceptions = 0;
}

template<std::floating_point T>
void ExceptionBuffer<T>::addException(EncodeException exception) {
    encodeException(exception, exceptionCount());
    ++exceptionCount();
}

template<std::floating_point T>
void ExceptionBuffer<T>::encodeException(EncodeException exception, common::offset_t idx) {
    static constexpr size_t exceptionSizeBytes = exception.sizeBytes();

    const auto byteOffset = idx * exceptionSizeBytes;
    KU_ASSERT(byteOffset + exceptionSizeBytes <= capacityBytes);
    std::memcpy(exceptions + byteOffset, &exception.value, sizeof(exception.value));
    std::memcpy(exceptions + byteOffset + sizeof(exception.value), &exception.posInPage,
        sizeof(exception.posInPage));
}

template<std::floating_point T>
ExceptionBuffer<T>::EncodeException ExceptionBuffer<T>::decodeException(common::offset_t idx) {
    EncodeException ret;

    static constexpr size_t exceptionSizeBytes = ret.sizeBytes();

    const auto byteOffset = idx * exceptionSizeBytes;
    KU_ASSERT(byteOffset + exceptionSizeBytes <= capacityBytes);
    std::memcpy(&ret.value, exceptions + byteOffset, sizeof(ret.value));
    std::memcpy(&ret.posInPage, exceptions + byteOffset + sizeof(ret.value), sizeof(ret.posInPage));

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
    return sizeof(Header) + exceptionCount() * EncodeException::sizeBytes();
}

template<std::floating_point T>
common::offset_t& ExceptionBuffer<T>::exceptionCount() {
    return header->numExceptions;
}

template<std::floating_point T>
size_t ExceptionBuffer<T>::getDataSizeForExceptions(size_t dataSize,
    const CompressionMetadata& metadata) {
    KU_ASSERT(metadata.children.size() >= 1);
    const size_t numValuesNoExceptions =
        metadata.getChild(0).numValues(dataSize, common::LogicalType::INT64());

    // TODO: fix
    return std::min(sizeof(Header) +
                        numValuesNoExceptions / MAX_EXCEPTION_FACTOR * EncodeException::sizeBytes(),
        static_cast<size_t>(31 * 128));
}

template<std::floating_point T>
size_t ExceptionBuffer<T>::getMaxExceptionCount(size_t exceptionBufferSize) {
    return (exceptionBufferSize - sizeof(Header)) / EncodeException::sizeBytes();
}
} // namespace

template<std::floating_point T>
uint64_t FloatCompression<T>::compressNextPage(const uint8_t*& srcBuffer,
    uint64_t numValuesRemaining, uint8_t* dstBuffer, uint64_t dstBufferSize,
    const struct CompressionMetadata& metadata) const {

    // TODO: this is hacky; we need a better system for dynamically choosing between
    // algorithms when compressing
    if (metadata.compression == CompressionType::UNCOMPRESSED) {
        return Uncompressed(sizeof(T)).compressNextPage(srcBuffer, numValuesRemaining, dstBuffer,
            dstBufferSize, metadata);
    }

    ExceptionBuffer<T> exceptionBuffer{dstBuffer, dstBufferSize, metadata};
    exceptionBuffer.init();
    KU_ASSERT(exceptionBuffer.capacityBytes < dstBufferSize);

    const size_t numValuesToCompress =
        std::min(numValuesRemaining, numValues(dstBufferSize, metadata));

    std::vector<EncodedType> integerEncodedValues(numValuesToCompress);
    for (size_t posInPage = 0; posInPage < numValuesToCompress; ++posInPage) {
        const auto floatValue = reinterpret_cast<const T*>(srcBuffer)[posInPage];
        integerEncodedValues[posInPage] = alp::AlpEncode<T>::encode_value(floatValue,
            metadata.alpMetadata.fac, metadata.alpMetadata.exp);

        const double decodedValue = alp::AlpDecode<T>::decode_value(integerEncodedValues[posInPage],
            metadata.alpMetadata.fac, metadata.alpMetadata.exp);
        if (floatValue != decodedValue) {
            KU_ASSERT(posInPage == static_cast<uint16_t>(posInPage));
            exceptionBuffer.addException(
                {.value = floatValue, .posInPage = static_cast<uint16_t>(posInPage)});

            // fill in the encoded value with the offset instead of 0 for better compression ratio
            const auto encodedIntegerInfo = encodedFloatBitpacker.getPackingInfo(metadata);
            integerEncodedValues[posInPage] = encodedIntegerInfo.offset;
        }
    }
    srcBuffer += numValuesToCompress * sizeof(T);

    const size_t encodedBufferSize = dstBufferSize - exceptionBuffer.capacityBytes;
    const auto* castedIntegerEncodedBuffer =
        reinterpret_cast<const uint8_t*>(integerEncodedValues.data());
    const auto compressedIntegerSize =
        encodedFloatBitpacker.compressNextPage(castedIntegerEncodedBuffer, numValuesToCompress,
            dstBuffer, encodedBufferSize, metadata.getChild(0));

    // Handle zeroing unused parts of the page here
    KU_ASSERT(compressedIntegerSize + exceptionBuffer.capacityBytes <= dstBufferSize);
    KU_ASSERT(encodedBufferSize >= compressedIntegerSize);
    memset(dstBuffer + compressedIntegerSize, 0, encodedBufferSize - compressedIntegerSize);

    const size_t encoded_exception_size = exceptionBuffer.getSizeBytes();
    KU_ASSERT(exceptionBuffer.capacityBytes >= encoded_exception_size);
    memset(reinterpret_cast<uint8_t*>(exceptionBuffer.header) + encoded_exception_size, 0,
        exceptionBuffer.capacityBytes - encoded_exception_size);

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
    const size_t exceptionBufferSize =
        ExceptionBuffer<T>::getDataSizeForExceptions(dataSize, metadata);
    const auto ret = decltype(encodedFloatBitpacker)::numValues(dataSize - exceptionBufferSize,
        metadata.getChild(0));
    if (ret == UINT64_MAX) {
        // handle case where there is only one non-exception value
        return ExceptionBuffer<T>::getMaxExceptionCount(dataSize);
    }
    return ret;
}

template<std::floating_point T>
void FloatCompression<T>::decompressFromPage(const uint8_t* srcBuffer, uint64_t srcOffset,
    uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues,
    const struct CompressionMetadata& metadata) const {

    std::vector<EncodedType> integerEncodedValues(numValues);
    encodedFloatBitpacker.decompressFromPage(srcBuffer, srcOffset,
        reinterpret_cast<uint8_t*>(integerEncodedValues.data()), 0, numValues,
        metadata.getChild(0));

    // TODO fix
    ExceptionBuffer<T> exceptionBuffer{(uint8_t*)srcBuffer, 4 * 1024, metadata};
    KU_ASSERT(exceptionBuffer.capacityBytes < 4 * 1024);
    for (size_t i = 0; i < numValues; ++i) {
        auto& dstValue = reinterpret_cast<T*>(dstBuffer)[dstOffset + i];

        // check if the current value is an exception
        bool isException = false;
        for (size_t j = 0; j < exceptionBuffer.exceptionCount(); ++j) {
            const auto currentException = exceptionBuffer.decodeException(j);
            if (currentException.posInPage == srcOffset + i) {
                dstValue = currentException.value;
                isException = true;
            }
        }

        if (!isException) {
            dstValue = alp::AlpDecode<T>::decode_value(integerEncodedValues[i],
                metadata.alpMetadata.fac, metadata.alpMetadata.exp);
        }
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
    for (size_t i = 0; i < numValues; ++i) {
        const size_t posInPage = i + dstOffset;
        const size_t posInSrc = i + srcOffset;

        const auto floatValue = reinterpret_cast<const T*>(srcBuffer)[posInSrc];
        integerEncodedValues[i] = alp::AlpEncode<T>::encode_value(floatValue,
            metadata.alpMetadata.fac, metadata.alpMetadata.exp);

        const double decodedValue = alp::AlpDecode<T>::decode_value(integerEncodedValues[i],
            metadata.alpMetadata.fac, metadata.alpMetadata.exp);
        if (floatValue != decodedValue) {
            bool posAlreadyException = false;
            for (size_t j = 0; j < exceptionBuffer.exceptionCount(); ++j) {
                auto currentException = exceptionBuffer.decodeException(j);
                if (currentException.posInPage == posInPage) {
                    currentException.value = floatValue;
                    exceptionBuffer.encodeException(currentException, j);
                    posAlreadyException = true;
                }
            }

            if (!posAlreadyException) {
                KU_ASSERT(posInPage == static_cast<uint16_t>(posInPage));
                exceptionBuffer.addException(
                    {.value = floatValue, .posInPage = static_cast<uint16_t>(posInPage)});
            }

            const auto encodedIntegerInfo = encodedFloatBitpacker.getPackingInfo(metadata);
            integerEncodedValues[i] = encodedIntegerInfo.offset;
        } else {
            // TODO clean this up
            for (size_t j = 0; j < exceptionBuffer.exceptionCount(); ++j) {
                const auto currentException = exceptionBuffer.decodeException(j);
                if (currentException.posInPage == posInPage) {
                    exceptionBuffer.removeException(j);
                    break;
                }
            }
        }
    }

    encodedFloatBitpacker.setValuesFromUncompressed(
        reinterpret_cast<const uint8_t*>(integerEncodedValues.data()), 0, dstBuffer, dstOffset,
        numValues, metadata.getChild(0), nullMask);
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
