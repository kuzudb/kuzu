#pragma once

#include "storage/compression/compression.h"
#include <concepts>
namespace kuzu {
namespace common {
class ValueVector;
class NullMask;
} // namespace common

namespace storage {
class ColumnChunkData;

struct PageCursor;

// Augmented with Frame of Reference encoding using an offset stored in the compression metadata
template<std::floating_point T>
class FloatCompression final : public CompressionAlg {
public:
    using EncodedType = int64_t;

public:
    FloatCompression() = default;

    static uint64_t getMaxExceptionCountPerPage(size_t pageSize,
        const CompressionMetadata& metadata);

    void setValuesFromUncompressed(const uint8_t* srcBuffer, common::offset_t srcOffset,
        uint8_t* dstBuffer, common::offset_t dstOffset, common::offset_t numValues,
        const CompressionMetadata& metadata, const common::NullMask* nullMask) const final;

    static uint64_t numValues(uint64_t dataSize, const CompressionMetadata& metadata);

    uint64_t compressNextPage(const uint8_t*& srcBuffer, uint64_t numValuesRemaining,
        uint8_t* dstBuffer, uint64_t dstBufferSize,
        const struct CompressionMetadata& metadata) const final;

    uint64_t compressNextPageWithExceptions(const uint8_t*& srcBuffer, uint64_t numValuesRemaining,
        uint8_t* dstBuffer, uint64_t dstBufferSize, uint8_t* exceptionBuffer,
        uint64_t exceptionBufferSize, uint64_t& exceptionCount,
        const struct CompressionMetadata& metadata) const {}

    void decompressFromPage(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues,
        const struct CompressionMetadata& metadata) const final;

    static bool canUpdateInPlace(std::span<const T> value, const CompressionMetadata& metadata,
        const std::optional<common::NullMask>& nullMask = std::nullopt,
        uint64_t nullMaskOffset = 0);

    CompressionType getCompressionType() const override { return CompressionType::FLOAT; }

    static BitpackInfo<int64_t> getBitpackInfo(const CompressionMetadata& metadata) {}

private:
    IntegerBitpacking<int64_t> encodedFloatBitpacker;
};

template<std::floating_point T>
struct EncodeException {
    T value;
    uint16_t posInPage;

    static constexpr size_t sizeBytes() {
        constexpr size_t exceptionSizeBytes = sizeof(value) + sizeof(posInPage);

        // best we can do to ensure that all fields are accounted for due to struct padding
        static_assert(exceptionSizeBytes + (8 - exceptionSizeBytes % 8) >= sizeof(EncodeException));

        return exceptionSizeBytes;
    }
};

template<std::floating_point T>
struct ExceptionBuffer {

    ExceptionBuffer(uint8_t* frame, size_t frameSizeBytes, const CompressionMetadata& metadata);
    struct Header {
        common::offset_t numExceptions;
    }* header;

    // this actually points to the last address in the exception buffer
    // as the exception buffer grows from the end of the page
    uint8_t* exceptions;
    size_t capacityBytes;

    void init();

    void addException(EncodeException<T> exception);
    void encodeException(EncodeException<T> exception, common::offset_t idx);
    EncodeException<T> decodeException(common::offset_t idx);
    void removeException(common::offset_t idx);
    size_t getSizeBytes();
    common::offset_t& exceptionCount();

    // at most 1 / MAX_EXCEPTION_FACTOR * totalCompressedValues can be exceptions
    // if this is not the case we avoid compressing altogether
    static constexpr size_t MAX_EXCEPTION_FACTOR = 8;

    static size_t getMaxExceptionRatio(size_t columnChunkExceptionRatio);
    static size_t getDataSizeForExceptions(size_t dataSize, const CompressionMetadata& metadata);
    static size_t getMaxExceptionCount(size_t exceptionBufferSize);
};

} // namespace storage
} // namespace kuzu
