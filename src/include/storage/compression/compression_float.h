#pragma once

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
    std::unique_ptr<CompressionAlg> getEncodedFloatBitpacker(
        const CompressionMetadata& metadata) const;
};

static_assert(std::is_same_v<FloatCompression<double>::EncodedType, int64_t>, "");
static_assert(std::is_same_v<FloatCompression<float>::EncodedType, int32_t>, "");

template<std::floating_point T>
struct EncodeException {
    T value;
    uint32_t posInChunk;

    static constexpr size_t sizeBytes() { return sizeof(value) + sizeof(posInChunk); }

    static size_t numPagesFromExceptions(size_t exceptionCount);

    static size_t exceptionBytesPerPage();

    bool operator<(const EncodeException<T>& o) const;
};

template<std::floating_point T>
struct EncodeExceptionView {
    explicit EncodeExceptionView(std::byte* val) { bytes = val; }

    EncodeException<T> getValue() const;
    void setValue(EncodeException<T> exception);
    std::byte* bytes;
};

} // namespace storage
} // namespace kuzu
