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
        common::offset_t posInPage; // TODO maybe make this a smaller type to store more exceptions
    };

    ExceptionBuffer(uint8_t* frame, size_t frameSizeBytes);
    struct Header {
        common::offset_t numExceptions;
    }* header;
    EncodeException* exceptions;
    size_t capacityBytes;

    void addException(EncodeException exception);
    void removeException(common::offset_t idx);
    size_t getSizeBytes();
    common::offset_t& exceptionCount();

    // at most 1 / MAX_EXCEPTION_FACTOR * totalCompressedValues can be exceptions
    // if this is not the case we avoid compressing altogether
    static constexpr size_t MAX_EXCEPTION_FACTOR = 8;

    static size_t getDataSizeForExceptions(size_t dataSize);
    static size_t getMaxExceptionCount(size_t exceptionBufferSize);
};

template<std::floating_point T>
ExceptionBuffer<T>::ExceptionBuffer(uint8_t* frame, size_t frameSizeBytes)
    : capacityBytes(getDataSizeForExceptions(frameSizeBytes)) {
    uint8_t* exceptionBufferStart = frame + frameSizeBytes - capacityBytes;
    header = reinterpret_cast<Header*>(exceptionBufferStart);
    exceptions = reinterpret_cast<EncodeException*>(exceptionBufferStart + sizeof(Header));
}

template<std::floating_point T>
void ExceptionBuffer<T>::addException(EncodeException exception) {
    KU_ASSERT(exceptionCount() < getMaxExceptionCount(capacityBytes));
    exceptions[exceptionCount()] = exception;
    ++exceptionCount();
}

template<std::floating_point T>
void ExceptionBuffer<T>::removeException(common::offset_t idx) {
    KU_ASSERT(idx < exceptionCount());
    for (size_t i = idx; i < exceptionCount() - 1; ++i) {
        exceptions[i] = exceptions[i + 1];
    }
    --exceptionCount();
}

template<std::floating_point T>
size_t ExceptionBuffer<T>::getSizeBytes() {
    return sizeof(Header) + exceptionCount() * sizeof(EncodeException);
}

template<std::floating_point T>
common::offset_t& ExceptionBuffer<T>::exceptionCount() {
    return header->numExceptions;
}

template<std::floating_point T>
size_t ExceptionBuffer<T>::getDataSizeForExceptions(size_t dataSize) {
    return dataSize / MAX_EXCEPTION_FACTOR;
}

template<std::floating_point T>
size_t ExceptionBuffer<T>::getMaxExceptionCount(size_t exceptionBufferSize) {
    return (exceptionBufferSize - sizeof(Header)) / sizeof(EncodeException);
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

    ExceptionBuffer<T> exceptionBuffer{dstBuffer, dstBufferSize};

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
            exceptionBuffer.addException({.value = floatValue, .posInPage = posInPage});

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
    memset(dstBuffer + compressedIntegerSize, 0, encodedBufferSize - compressedIntegerSize);

    const size_t encoded_exception_size = exceptionBuffer.getSizeBytes();
    memset(reinterpret_cast<uint8_t*>(exceptionBuffer.header) + encoded_exception_size, 0,
        exceptionBuffer.capacityBytes - encoded_exception_size);

    // since we already do the zeroing we return the size of the whole page
    return dstBufferSize;
}

template<std::floating_point T>
uint64_t FloatCompression<T>::getMaxExceptionCountPerPage(size_t pageSize) {
    return ExceptionBuffer<T>::getMaxExceptionCount(pageSize);
}

template<std::floating_point T>
uint64_t FloatCompression<T>::numValues(uint64_t dataSize, const CompressionMetadata& metadata) {
    const size_t exceptionBufferSize = ExceptionBuffer<T>::getDataSizeForExceptions(dataSize);
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
    ExceptionBuffer<T> exceptionBuffer{(uint8_t*)srcBuffer, 4 * 1024};
    for (size_t i = 0; i < numValues; ++i) {
        auto& dstValue = reinterpret_cast<T*>(dstBuffer)[dstOffset + i];

        // check if the current value is an exception
        bool isException = false;
        for (size_t j = 0; j < exceptionBuffer.exceptionCount(); ++j) {
            const auto currentException = exceptionBuffer.exceptions[j];
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
    ExceptionBuffer<T> exceptionBuffer{dstBuffer, 4 * 1024};

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
                auto& curException = exceptionBuffer.exceptions[j];
                if (curException.posInPage == posInPage) {
                    curException.value = floatValue;
                    posAlreadyException = true;
                }
            }

            if (!posAlreadyException) {
                exceptionBuffer.addException({.value = floatValue, .posInPage = posInPage});
            }

            const auto encodedIntegerInfo = encodedFloatBitpacker.getPackingInfo(metadata);
            integerEncodedValues[i] = encodedIntegerInfo.offset;
        } else {
            // TODO clean this up
            for (size_t j = 0; j < exceptionBuffer.exceptionCount(); ++j) {
                auto& curException = exceptionBuffer.exceptions[j];
                if (curException.posInPage == posInPage) {
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
