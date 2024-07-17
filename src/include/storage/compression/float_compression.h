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
class FloatCompression final : public CompressionAlg {
public:
    using EncodedType = std::conditional_t<std::is_same_v<T, double>, int64_t, int32_t>;

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

    CompressionType getCompressionType() const override { return CompressionType::ALP; }

    static BitpackInfo<EncodedType> getBitpackInfo(const CompressionMetadata& metadata);

private:
    std::unique_ptr<CompressionAlg> getEncodedFloatBitpacker(
        const CompressionMetadata& metadata) const;
};

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
    // Used to access ALP exceptions that are stored in buffers
    // We don't use the EncodeException struct directly since we don't want to copy struct padding
    explicit EncodeExceptionView(std::byte* val) { bytes = val; }

    EncodeException<T> getValue() const;
    void setValue(EncodeException<T> exception);
    std::byte* bytes;
};

} // namespace storage
} // namespace kuzu
