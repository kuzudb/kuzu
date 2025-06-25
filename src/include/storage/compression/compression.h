#pragma once

#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <type_traits>

#include "alp/state.hpp"
#include "common/assert.h"
#include "common/null_mask.h"
#include "common/numeric_utils.h"
#include "common/system_config.h"
#include "common/types/types.h"
#include <span>

namespace kuzu {
namespace common {
class ValueVector;
class NullMask;
} // namespace common

namespace storage {
class ColumnChunkData;

struct PageCursor;

template<typename T>
concept StorageValueType = (common::numeric_utils::IsIntegral<T> || std::floating_point<T>);
// Type storing values in the column chunk statistics
// Only supports integers (up to 128bit), floats and bools
union StorageValue {
    int64_t signedInt;
    uint64_t unsignedInt;
    double floatVal;
    common::int128_t signedInt128;

    StorageValue() = default;
    template<typename T>
        requires std::same_as<std::remove_cvref_t<T>, common::int128_t>
    explicit StorageValue(T value) : signedInt128(value) {}

    template<typename T>
        requires std::integral<T> && std::numeric_limits<T>::is_signed
    // zero-initilize union padding
    explicit StorageValue(T value) : StorageValue(common::int128_t(0)) {
        signedInt = value;
    }

    template<typename T>
        requires std::integral<T> && (!std::numeric_limits<T>::is_signed)
    explicit StorageValue(T value) : StorageValue(common::int128_t(0)) {
        unsignedInt = value;
    }

    template<typename T>
        requires std::is_floating_point_v<T>
    explicit StorageValue(T value) : StorageValue(common::int128_t(0)) {
        floatVal = value;
    }

    bool operator==(const StorageValue& other) const {
        // We zero-initialize any padding bits, so we can compare values to check equality
        return this->signedInt128 == other.signedInt128;
    }

    template<StorageValueType T>
    StorageValue& operator=(const T& val) {
        return *this = StorageValue(val);
    }

    template<StorageValueType T>
    T get() const {
        if constexpr (std::same_as<std::remove_cvref_t<T>, common::int128_t>) {
            return signedInt128;
        } else if constexpr (std::integral<T>) {
            if constexpr (std::numeric_limits<T>::is_signed) {
                return static_cast<T>(signedInt);
            } else {
                return static_cast<T>(unsignedInt);
            }
        } else if constexpr (std::is_floating_point<T>()) {
            return floatVal;
        } else {
            KU_UNREACHABLE;
        }
    }

    bool gt(const StorageValue& other, common::PhysicalTypeID type) const;

    // If the type cannot be stored in the statistics, readFromVector will return nullopt
    static std::optional<StorageValue> readFromVector(const common::ValueVector& vector,
        common::offset_t posInVector);
};
static_assert(std::is_trivial_v<StorageValue>);

std::pair<std::optional<StorageValue>, std::optional<StorageValue>> getMinMaxStorageValue(
    const uint8_t* data, uint64_t offset, uint64_t numValues, common::PhysicalTypeID physicalType,
    const common::NullMask* nullMask, bool valueRequiredIfUnsupported = false);

// Returns the size of the data type in bytes
uint32_t getDataTypeSizeInChunk(const common::LogicalType& dataType);
uint32_t getDataTypeSizeInChunk(const common::PhysicalTypeID& dataType);

// Compression type is written to the data header both so we can usually catch issues when we
// decompress uncompressed data by mistake, and to allow for runtime-configurable compression.
enum class CompressionType : uint8_t {
    UNCOMPRESSED = 0,
    INTEGER_BITPACKING = 1,
    BOOLEAN_BITPACKING = 2,
    CONSTANT = 3,
    ALP = 4,
};

struct ExtraMetadata {
    virtual ~ExtraMetadata() = default;
    virtual std::unique_ptr<ExtraMetadata> copy() = 0;
};

// used only for compressing floats/doubles
struct ALPMetadata : ExtraMetadata {
    ALPMetadata() : exp(0), fac(0), exceptionCount(0), exceptionCapacity(0) {}
    explicit ALPMetadata(const alp::state& alpState, common::PhysicalTypeID physicalType);

    uint8_t exp;
    uint8_t fac;
    uint32_t exceptionCount;
    uint32_t exceptionCapacity;

    void serialize(common::Serializer& serializer) const;
    static ALPMetadata deserialize(common::Deserializer& deserializer);

    std::unique_ptr<ExtraMetadata> copy() override;
};

struct InPlaceUpdateLocalState {
    struct FloatState {
        size_t newExceptionCount;
    } floatState;
};

// Data statistics used for determining how to handle compressed data
struct KUZU_API CompressionMetadata {

    // Minimum and maximum are upper and lower bounds for the data.
    // Updates and deletions may cause them to no longer be the exact minimums and maximums,
    // but no value will be larger than the maximum or smaller than the minimum
    StorageValue min;
    StorageValue max;

    CompressionType compression;

    // TODO: maybe this should be stored inside the segment. The metadata should be as minimal as
    // possible
    std::optional<std::unique_ptr<ExtraMetadata>> extraMetadata;
    // TODO: This will be removed since the ALP exception data will be stored inside the segment
    std::vector<CompressionMetadata> children;

    CompressionMetadata(StorageValue min, StorageValue max, CompressionType compression)
        : min(min), max(max), compression(compression), extraMetadata() {}

    // constructor for float metadata
    CompressionMetadata(StorageValue min, StorageValue max, CompressionType compression,
        const alp::state& state, StorageValue minEncoded, StorageValue maxEncoded,
        common::PhysicalTypeID physicalType);

    CompressionMetadata(const CompressionMetadata&);
    CompressionMetadata& operator=(const CompressionMetadata&);

    static size_t getChildCount(CompressionType compressionType);

    inline bool isConstant() const { return compression == CompressionType::CONSTANT; }
    const CompressionMetadata& getChild(common::offset_t idx) const;

    // accessors for additionalMetadata
    inline const ExtraMetadata* getExtraMetadata() const {
        KU_ASSERT(extraMetadata.has_value());
        return extraMetadata.value().get();
    }
    inline ExtraMetadata* getExtraMetadata() {
        KU_ASSERT(extraMetadata.has_value());
        return extraMetadata.value().get();
    }
    inline const ALPMetadata* floatMetadata() const {
        return common::ku_dynamic_cast<const ALPMetadata*>(getExtraMetadata());
    }
    inline ALPMetadata* floatMetadata() {
        return common::ku_dynamic_cast<ALPMetadata*>(getExtraMetadata());
    }

    void serialize(common::Serializer& serializer) const;
    static CompressionMetadata deserialize(common::Deserializer& deserializer);

    // Returns true if and only if the provided value within the vector can be updated
    // in this chunk in-place.
    bool canUpdateInPlace(const uint8_t* data, uint32_t pos, uint64_t numValues,
        common::PhysicalTypeID physicalType, InPlaceUpdateLocalState& localUpdateState,
        const std::optional<common::NullMask>& nullMask = std::nullopt) const;
    bool canAlwaysUpdateInPlace() const;

    std::string toString(const common::PhysicalTypeID physicalType) const;
};

class CompressionAlg {
public:
    virtual ~CompressionAlg() = default;

    // Takes a single uncompressed value from the srcBuffer and compresses it into the dstBuffer
    // Offsets refer to value offsets, not byte offsets
    //
    // nullMask may be null if no mask is available (all values are non-null)
    // Storage of null values is handled by the implementation and decompression of null values
    // does not have to produce the original value passed to this function.
    //
    // dstBuffer points to an entire segment
    virtual void setValuesFromUncompressed(const uint8_t* srcBuffer, common::offset_t srcOffset,
        std::span<uint8_t> dstBuffer, common::offset_t dstOffset, common::offset_t numValues,
        const CompressionMetadata& metadata, const common::NullMask* nullMask) const = 0;

    // Takes uncompressed data from the srcBuffer and compresses it into the dstBuffer
    // All values should be able to fit in the buffer, and the result will be stored as an entire
    // segment
    // TODO(bmwinger): is there any way of re-using the buffers from one segment to the next?
    // Otherwise we should just assume that the buffer is already zeroed
    virtual void compress(const uint8_t* srcBuffer, uint64_t numValues,
        std::span<uint8_t> dstBuffer, const struct CompressionMetadata& metadata) const = 0;

    // Takes compressed data from the srcBuffer and decompresses it into the dstBuffer
    // Offsets refer to value offsets, not byte offsets
    // srcBuffer points to the entire segment
    virtual void decompress(std::span<const uint8_t> srcBuffer, uint64_t srcOffset,
        uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues,
        const CompressionMetadata& metadata) const = 0;

    virtual CompressionType getCompressionType() const = 0;
};

class ConstantCompression final : public CompressionAlg {
public:
    explicit ConstantCompression(const common::LogicalType& logicalType)
        : numBytesPerValue{static_cast<uint8_t>(getDataTypeSizeInChunk(logicalType))},
          dataType{logicalType.getPhysicalType()} {}
    static std::optional<CompressionMetadata> analyze(const ColumnChunkData& chunk);

    // Shouldn't be used, there's a special case when compressing which ends early for constant
    // compression
    void compress(const uint8_t*, uint64_t, std::span<uint8_t>,
        const struct CompressionMetadata&) const override {};

    static void decompressValues(uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues,
        common::PhysicalTypeID physicalType, uint32_t numBytesPerValue,
        const CompressionMetadata& metadata);

    void decompress(std::span<const uint8_t> /*srcBuffer*/, uint64_t /*srcOffset*/,
        uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues,
        const CompressionMetadata& metadata) const override;

    void copy(uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues,
        const CompressionMetadata& metadata) const;

    // Nothing to do; constant compressed data is only updated if the update is to the same value
    void setValuesFromUncompressed(const uint8_t*, common::offset_t, std::span<uint8_t>,
        common::offset_t, common::offset_t, const CompressionMetadata&,
        const common::NullMask* /*nullMask*/) const override {}

    CompressionType getCompressionType() const override { return CompressionType::CONSTANT; }

    static uint64_t getMaxCapacity(common::page_idx_t, common::PhysicalTypeID,
        CompressionMetadata) {
        return UINT64_MAX;
    }

private:
    uint8_t numBytesPerValue;
    common::PhysicalTypeID dataType;
};

// Compression alg which does not compress values and instead just copies them.
class Uncompressed : public CompressionAlg {
public:
    explicit Uncompressed(common::PhysicalTypeID physicalType)
        : numBytesPerValue{getDataTypeSizeInChunk(physicalType)} {}
    explicit Uncompressed(const common::LogicalType& logicalType)
        : Uncompressed(logicalType.getPhysicalType()) {}
    explicit Uncompressed(uint8_t numBytesPerValue) : numBytesPerValue{numBytesPerValue} {}

    Uncompressed(const Uncompressed&) = default;

    void setValuesFromUncompressed(const uint8_t* srcBuffer, common::offset_t srcOffset,
        std::span<uint8_t> dstBuffer, common::offset_t dstOffset, common::offset_t numValues,
        const CompressionMetadata& /*metadata*/, const common::NullMask* /*nullMask*/) const final {
        memcpy(dstBuffer.data() + dstOffset * numBytesPerValue,
            srcBuffer + srcOffset * numBytesPerValue, numBytesPerValue * numValues);
    }

    static uint64_t numValues(uint64_t dataSize, common::PhysicalTypeID physicalType);
    static uint64_t numValues(uint64_t dataSize, const common::LogicalType& logicalType);
    static uint64_t getMinPages(uint64_t capacity, common::PhysicalTypeID physicalType);

    void compress(const uint8_t* srcBuffer, uint64_t numValues, std::span<uint8_t> dstBuffer,
        const struct CompressionMetadata& /*metadata*/) const override {
        if (numBytesPerValue == 0) {
            return;
        }
        uint64_t sizeToCopy = numValues * numBytesPerValue;
        KU_ASSERT(sizeToCopy <= dstBuffer.size());
        std::memcpy(dstBuffer.data(), srcBuffer, sizeToCopy);
    }

    void decompress(std::span<const uint8_t> srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues,
        const CompressionMetadata& /*metadata*/) const override {
        std::memcpy(dstBuffer + dstOffset * numBytesPerValue,
            srcBuffer.data() + srcOffset * numBytesPerValue, numValues * numBytesPerValue);
    }

    CompressionType getCompressionType() const override { return CompressionType::UNCOMPRESSED; }

    static uint64_t getMaxCapacity(common::page_idx_t numPages, common::PhysicalTypeID dataType) {
        auto numBytesPerValue = getDataTypeSizeInChunk(dataType);
        if (numBytesPerValue == 0) {
            return UINT64_MAX;
        }
        return numPages * common::KUZU_PAGE_SIZE / numBytesPerValue;
    }

protected:
    const uint32_t numBytesPerValue;
};

template<typename T>
struct BitpackInfo {
    uint8_t bitWidth;
    bool hasNegative;
    T offset;
};

template<typename T>
concept IntegerBitpackingType = (common::numeric_utils::IsIntegral<T> && !std::same_as<T, bool>);

// Augmented with Frame of Reference encoding using an offset stored in the compression metadata
template<IntegerBitpackingType T>
class IntegerBitpacking : public CompressionAlg {
    using U = common::numeric_utils::MakeUnSignedT<T>;

public:
    // This is an implementation detail of the fastpfor bitpacking algorithm
    static constexpr uint64_t CHUNK_SIZE = 32;

public:
    IntegerBitpacking() = default;
    IntegerBitpacking(const IntegerBitpacking&) = default;

    void setValuesFromUncompressed(const uint8_t* srcBuffer, common::offset_t srcOffset,
        std::span<uint8_t> dstBuffer, common::offset_t dstOffset, common::offset_t numValues,
        const CompressionMetadata& metadata, const common::NullMask* nullMask) const final;

    static BitpackInfo<T> getPackingInfo(const CompressionMetadata& metadata);

    static inline uint64_t numValues(uint64_t dataSize, const BitpackInfo<T>& info) {
        if (info.bitWidth == 0) {
            return UINT64_MAX;
        }
        auto numValues = dataSize * 8 / info.bitWidth;
        return numValues;
    }

    static inline uint64_t numValues(uint64_t dataSize, const CompressionMetadata& metadata) {
        auto info = getPackingInfo(metadata);
        return numValues(dataSize, info);
    }

    void compress(const uint8_t* srcBuffer, uint64_t numValues, std::span<uint8_t> dstBuffer,
        const struct CompressionMetadata& metadata) const final;

    void decompress(std::span<const uint8_t> srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues,
        const struct CompressionMetadata& metadata) const final;

    static bool canUpdateInPlace(std::span<const T> value, const CompressionMetadata& metadata,
        const std::optional<common::NullMask>& nullMask = std::nullopt,
        uint64_t nullMaskOffset = 0);

    CompressionType getCompressionType() const override {
        return CompressionType::INTEGER_BITPACKING;
    }

    static uint64_t getMaxCapacity(common::page_idx_t numPages, CompressionMetadata compMeta);
    static uint64_t getMinPages(uint64_t capacity, CompressionMetadata compMeta);

protected:
    // Read multiple values from within a chunk. Cannot span multiple chunks.
    void getValues(const uint8_t* chunkStart, uint8_t pos, uint8_t* dst, uint8_t numValuesToRead,
        const BitpackInfo<T>& header) const;

    inline const uint8_t* getChunkStart(const uint8_t* buffer, uint64_t pos,
        uint8_t bitWidth) const {
        // Order of operations is important so that pos is rounded down to a multiple of
        // CHUNK_SIZE
        return buffer + (pos / CHUNK_SIZE) * bitWidth * CHUNK_SIZE / 8;
    }

    void packPartialChunk(const U* srcBuffer, uint8_t* dstBuffer, size_t posInDst,
        BitpackInfo<T> info, size_t remainingValues) const;

    void copyValuesToTempChunkWithOffset(const U* srcBuffer, U* tmpBuffer, BitpackInfo<T> info,
        size_t numValuesToCopy) const;

    void setPartialChunkInPlace(const uint8_t* srcBuffer, common::offset_t posInSrc,
        uint8_t* dstBuffer, common::offset_t posInDst, common::offset_t numValues,
        const BitpackInfo<T>& header) const;
};

class BooleanBitpacking : public CompressionAlg {
public:
    BooleanBitpacking() = default;
    BooleanBitpacking(const BooleanBitpacking&) = default;

    void setValuesFromUncompressed(const uint8_t* srcBuffer, common::offset_t srcOffset,
        std::span<uint8_t> dstBuffer, common::offset_t dstOffset, common::offset_t numValues,
        const CompressionMetadata& metadata, const common::NullMask* nullMask) const final;

    static inline uint64_t numValues(uint64_t dataSize) { return dataSize * 8; }

    void compress(const uint8_t* srcBuffer, uint64_t numValues, std::span<uint8_t> dstBuffer,
        const struct CompressionMetadata& metadata) const final;

    void decompress(std::span<const uint8_t> srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer,
        uint64_t dstOffset, uint64_t numValues, const CompressionMetadata& metadata) const final;

    void copy(const uint8_t* srcBuffer, uint64_t srcOffset, uint8_t* dstBuffer, uint64_t dstOffset,
        uint64_t numValues, const CompressionMetadata& metadata) const;

    CompressionType getCompressionType() const override {
        return CompressionType::BOOLEAN_BITPACKING;
    }

    static uint64_t getMaxCapacity(common::page_idx_t numPages) {
        return numPages * common::KUZU_PAGE_SIZE * 8;
    }
};

class CompressedFunctor {
public:
    CompressedFunctor(const CompressedFunctor&) = default;

protected:
    explicit CompressedFunctor(const common::LogicalType& logicalType)
        : constant{logicalType}, uncompressed{logicalType},
          physicalType{logicalType.getPhysicalType()} {}
    const ConstantCompression constant;
    const Uncompressed uncompressed;
    const BooleanBitpacking booleanBitpacking;
    const common::PhysicalTypeID physicalType;
};

class ReadCompressedValuesToVector : public CompressedFunctor {
public:
    explicit ReadCompressedValuesToVector(const common::LogicalType& logicalType)
        : CompressedFunctor(logicalType) {}
    ReadCompressedValuesToVector(const ReadCompressedValuesToVector&) = default;

    void operator()(std::span<const uint8_t> segment, uint64_t startOffset,
        common::ValueVector* resultVector, uint32_t posInVector, uint64_t numValuesToRead,
        const CompressionMetadata& metadata);
};

class ReadCompressedValues : public CompressedFunctor {
public:
    explicit ReadCompressedValues(const common::LogicalType& logicalType)
        : CompressedFunctor(logicalType) {}
    ReadCompressedValues(const ReadCompressedValues&) = default;

    void operator()(std::span<const uint8_t> segment, uint64_t startOffset, uint8_t* result,
        uint32_t startPosInResult, uint64_t numValuesToRead, const CompressionMetadata& metadata);
};

class WriteCompressedValues : public CompressedFunctor {
public:
    explicit WriteCompressedValues(const common::LogicalType& logicalType)
        : CompressedFunctor(logicalType) {}
    WriteCompressedValues(const WriteCompressedValues&) = default;

    void operator()(std::span<uint8_t> segment, uint64_t posInSegment, const uint8_t* data,
        common::offset_t dataOffset, common::offset_t numValues,
        const CompressionMetadata& metadata, const common::NullMask* nullMask = nullptr);

    void operator()(std::span<uint8_t> segment, uint64_t posInSegment, common::ValueVector* vector,
        uint32_t posInVector, common::offset_t numValues, const CompressionMetadata& metadata);
};

} // namespace storage
} // namespace kuzu
