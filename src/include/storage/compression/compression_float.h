#pragma once

#include <limits>
#include <type_traits>

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

template<std::floating_point T>
decltype(auto) getBitpackingType() {
    if constexpr (std::is_same_v<T, double>) {
        return int64_t{};
    } else {
        return int32_t{};
    }
}

// Augmented with Frame of Reference encoding using an offset stored in the compression metadata
template<std::floating_point T>
class FloatCompression final : public CompressionAlg {
public:
    // TODO change to int32_t for floats
    using EncodedType = decltype(getBitpackingType<T>());

public:
    FloatCompression() = default;

    void setValuesFromUncompressed(const uint8_t* srcBuffer, common::offset_t srcOffset,
        uint8_t* dstBuffer, common::offset_t dstOffset, common::offset_t numValues,
        const CompressionMetadata& metadata, const common::NullMask* nullMask) const final;

    static uint64_t numValues(uint64_t dataSize, const CompressionMetadata& metadata);

    // this is included to satisfy the CompressionAlg interface but we don't actually use it
    uint64_t compressNextPage(const uint8_t*& srcBuffer, uint64_t numValuesRemaining,
        uint8_t* dstBuffer, uint64_t dstBufferSize,
        const struct CompressionMetadata& metadata) const final;

    uint64_t compressNextPageWithExceptions(const uint8_t*& srcBuffer, uint64_t srcOffset,
        uint64_t numValuesRemaining, uint8_t* dstBuffer, uint64_t dstBufferSize,
        uint8_t* exceptionBuffer, uint64_t exceptionBufferSize, uint64_t& exceptionCount,
        const struct CompressionMetadata& metadata) const;

    // does not patch exceptions (this is handled by the column reader)
    void decompressFromPage(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues,
        const struct CompressionMetadata& metadata) const final;

    static bool canUpdateInPlace(std::span<const T> value, const CompressionMetadata& metadata,
        CompressionMetadata::InPlaceUpdateLocalState& localUpdateState,
        const std::optional<common::NullMask>& nullMask = std::nullopt,
        uint64_t nullMaskOffset = 0);

    CompressionType getCompressionType() const override { return CompressionType::FLOAT; }

    static BitpackInfo<EncodedType> getBitpackInfo(const CompressionMetadata& metadata);

private:
    IntegerBitpacking<EncodedType> encodedFloatBitpacker;
};

static_assert(std::is_same_v<FloatCompression<double>::EncodedType, int64_t>, "");
static_assert(std::is_same_v<FloatCompression<float>::EncodedType, int32_t>, "");

template<std::floating_point T>
struct EncodeException {
    T value;
    uint32_t posInChunk;

    static constexpr auto INVALID_POS = std::numeric_limits<decltype(posInChunk)>::max();

    static constexpr size_t sizeBytes() {
        constexpr size_t exceptionSizeBytes = sizeof(value) + sizeof(posInChunk);

        // best we can do to ensure that all fields are accounted for due to struct padding
        static_assert(exceptionSizeBytes + (8 - exceptionSizeBytes % 8) >= sizeof(EncodeException));

        return exceptionSizeBytes;
    }

    bool operator<(const EncodeException<T>& o) const {
        KU_ASSERT(posInChunk != o.posInChunk);
        return posInChunk < o.posInChunk;
    }
};

template<std::floating_point T>
struct ExceptionBufferElementView {
    using type = EncodeException<T>;

    explicit ExceptionBufferElementView(std::byte* val) { bytes = val; }

    EncodeException<T> getValue() const;
    void setValue(EncodeException<T> exception);
    std::byte* bytes;
};

} // namespace storage
} // namespace kuzu
