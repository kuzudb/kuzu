#include "storage/compression/float_compression.h"

#include "alp/encode.hpp"
#include "common/assert.h"
#include "common/cast.h"
#include "common/system_config.h"
#include "common/utils.h"
#include "storage/compression/compression.h"
#include <ranges>

namespace kuzu {
namespace storage {

namespace {
static constexpr common::idx_t BITPACKING_CHILD_IDX = 0;

template<std::floating_point T>
common::LogicalType getBitpackingLogicalType() {
    if constexpr (std::is_same_v<T, float>) {
        return common::LogicalType::INT32();
    } else {
        return common::LogicalType::INT64();
    }
}
} // namespace

template<std::floating_point T>
size_t EncodeException<T>::numPagesFromExceptions(size_t exceptionCount) {
    return common::ceilDiv(static_cast<uint64_t>(exceptionCount) * sizeInBytes(),
        common::KUZU_PAGE_SIZE);
}

template<std::floating_point T>
size_t EncodeException<T>::exceptionBytesPerPage() {
    return common::KUZU_PAGE_SIZE / sizeInBytes() * sizeInBytes();
}

template<std::floating_point T>
bool EncodeException<T>::operator<(const EncodeException<T>& o) const {
    return posInChunk < o.posInChunk;
}

template<std::floating_point T>
EncodeException<T> EncodeExceptionView<T>::getValue(common::offset_t elementOffset) const {
    EncodeException<T> ret{};
    const auto* const elementAddress = bytes + elementOffset * decltype(ret)::sizeInBytes();
    std::memcpy(&ret.value, elementAddress, sizeof(ret.value));
    std::memcpy(&ret.posInChunk, elementAddress + sizeof(ret.value), sizeof(ret.posInChunk));
    return ret;
}

template<std::floating_point T>
void EncodeExceptionView<T>::setValue(EncodeException<T> exception,
    common::offset_t elementOffset) {
    auto* const elementAddress = bytes + elementOffset * decltype(exception)::sizeInBytes();
    std::memcpy(elementAddress, &exception.value, sizeof(exception.value));
    std::memcpy(elementAddress + sizeof(exception.value), &exception.posInChunk,
        sizeof(exception.posInChunk));
}

template<std::floating_point T>
FloatCompression<T>::FloatCompression()
    : constantEncodedFloatBitpacker(getBitpackingLogicalType<T>()), encodedFloatBitpacker() {}

template<std::floating_point T>
void FloatCompression<T>::compress(const uint8_t*, uint64_t, std::span<uint8_t>,
    const struct CompressionMetadata&) const {
    KU_UNREACHABLE;
}

template<std::floating_point T>
void FloatCompression<T>::compressWithExceptions(const uint8_t* srcBuffer, uint64_t srcOffset,
    uint64_t numValues, std::span<uint8_t> dstBuffer, EncodeExceptionView<T> exceptionBuffer,
    [[maybe_unused]] uint64_t exceptionBufferSize, uint64_t& exceptionCount,
    const struct CompressionMetadata& metadata) const {
    KU_ASSERT(metadata.compression == CompressionType::ALP);

    RUNTIME_CHECK({
        auto capacity =
            IntegerBitpacking<EncodedType>::numValues(dstBuffer.size(), metadata.getChild(0));
        auto minPages =
            IntegerBitpacking<EncodedType>::getMinPages(numValues, metadata.getChild(0));
        KU_ASSERT(numValues <= capacity);
        KU_ASSERT(dstBuffer.size() / common::KUZU_PAGE_SIZE >= minPages);
    });

    std::vector<EncodedType> integerEncodedValues(numValues);
    for (size_t posInSegment = 0; posInSegment < numValues; ++posInSegment) {
        const auto floatValue = reinterpret_cast<const T*>(srcBuffer)[posInSegment];
        const auto* floatMetadata = metadata.floatMetadata();
        const EncodedType encodedValue =
            alp::AlpEncode<T>::encode_value(floatValue, floatMetadata->fac, floatMetadata->exp);
        const double decodedValue =
            alp::AlpDecode<T>::decode_value(encodedValue, floatMetadata->fac, floatMetadata->exp);

        if (floatValue != decodedValue) {
            KU_ASSERT(
                (exceptionCount + 1) * EncodeException<T>::sizeInBytes() <= exceptionBufferSize);
            exceptionBuffer.setValue({.value = floatValue,
                                         .posInChunk = common::safeIntegerConversion<uint32_t>(
                                             srcOffset + posInSegment)},
                exceptionCount);

            // We don't need to replace with 1st successful encode as the integer bitpacking
            // metadata is already populated
            ++exceptionCount;
        } else {
            integerEncodedValues[posInSegment] = encodedValue;
        }
    }

    const auto* castedIntegerEncodedBuffer =
        reinterpret_cast<const uint8_t*>(integerEncodedValues.data());
    getEncodedFloatBitpacker(metadata).compress(castedIntegerEncodedBuffer, numValues, dstBuffer,
        metadata.getChild(BITPACKING_CHILD_IDX));
}

template<std::floating_point T>
uint64_t FloatCompression<T>::numValues(uint64_t dataSize, const CompressionMetadata& metadata) {
    return IntegerBitpacking<EncodedType>::numValues(dataSize,
        metadata.getChild(BITPACKING_CHILD_IDX));
}

template<std::floating_point T>
void FloatCompression<T>::decompress(std::span<const uint8_t> srcBuffer, uint64_t srcOffset,
    uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues,
    const struct CompressionMetadata& metadata) const {

    // use dstBuffer for unpacking the ALP encoded values then decode them in place
    getEncodedFloatBitpacker(metadata).decompress(srcBuffer, srcOffset, dstBuffer, dstOffset,
        numValues, metadata.getChild(BITPACKING_CHILD_IDX));

    static_assert(sizeof(EncodedType) == sizeof(T));
    auto* integerEncodedValues = reinterpret_cast<EncodedType*>(dstBuffer);
    for (size_t i = 0; i < numValues; ++i) {
        reinterpret_cast<T*>(dstBuffer)[dstOffset + i] =
            alp::AlpDecode<T>::decode_value(integerEncodedValues[dstOffset + i],
                metadata.floatMetadata()->fac, metadata.floatMetadata()->exp);
    }
}

template<std::floating_point T>
void FloatCompression<T>::setValuesFromUncompressed(const uint8_t* srcBuffer,
    common::offset_t srcOffset, std::span<uint8_t> dstBuffer, common::offset_t dstOffset,
    common::offset_t numValues, const CompressionMetadata& metadata,
    const common::NullMask* nullMask) const {
    // each individual value that is being updated should be able to be updated in place
    RUNTIME_CHECK(InPlaceUpdateLocalState localUpdateState{});
    KU_ASSERT(numValues ==
              static_cast<common::offset_t>(
                  std::ranges::count_if(std::ranges::iota_view{srcOffset, srcOffset + numValues},
                      [&localUpdateState, srcBuffer, &metadata, nullMask](common::offset_t i) {
                          auto value = reinterpret_cast<const T*>(srcBuffer)[i];
                          return (nullMask && nullMask->isNull(i)) ||
                                 canUpdateInPlace(std::span(&value, 1), metadata, localUpdateState);
                      })));

    std::vector<EncodedType> integerEncodedValues(numValues);
    for (size_t i = 0; i < numValues; ++i) {
        const size_t posInSrc = i + srcOffset;

        const auto floatValue = reinterpret_cast<const T*>(srcBuffer)[posInSrc];
        const EncodedType encodedValue = alp::AlpEncode<T>::encode_value(floatValue,
            metadata.floatMetadata()->fac, metadata.floatMetadata()->exp);
        integerEncodedValues[i] = encodedValue;
    }

    getEncodedFloatBitpacker(metadata).setValuesFromUncompressed(
        reinterpret_cast<const uint8_t*>(integerEncodedValues.data()), 0, dstBuffer, dstOffset,
        numValues, metadata.getChild(BITPACKING_CHILD_IDX), nullMask);
}

template<std::floating_point T>
const CompressionAlg& FloatCompression<T>::getEncodedFloatBitpacker(
    const CompressionMetadata& metadata) const {
    if (metadata.getChild(BITPACKING_CHILD_IDX).isConstant()) {
        return constantEncodedFloatBitpacker;
    } else {
        return encodedFloatBitpacker;
    }
}

template<std::floating_point T>
BitpackInfo<typename FloatCompression<T>::EncodedType> FloatCompression<T>::getBitpackInfo(
    const CompressionMetadata& metadata) {
    const auto& bitpackMetadata = metadata.getChild(BITPACKING_CHILD_IDX);
    if (bitpackMetadata.isConstant()) {
        const auto constValue = bitpackMetadata.min.get<EncodedType>();
        return {.bitWidth = 0, .hasNegative = (constValue < 0), .offset = constValue};
    } else {
        return IntegerBitpacking<EncodedType>::getPackingInfo(bitpackMetadata);
    }
}

template<std::floating_point T>
bool FloatCompression<T>::canUpdateInPlace(std::span<const T> value,
    const CompressionMetadata& metadata, InPlaceUpdateLocalState& localUpdateState,
    const std::optional<common::NullMask>& nullMask, uint64_t nullMaskOffset) {
    size_t newExceptionCount = 0;
    std::vector<EncodedType> encodedValues(value.size());
    const auto bitpackingInfo = getBitpackInfo(metadata);
    const auto* floatMetadata = metadata.floatMetadata();
    for (size_t i = 0; i < value.size(); ++i) {
        if (nullMask && nullMask->isNull(nullMaskOffset + i)) {
            continue;
        }

        const auto floatValue = value[i];
        const EncodedType encodedValue =
            alp::AlpEncode<T>::encode_value(floatValue, floatMetadata->fac, floatMetadata->exp);
        const double decodedValue =
            alp::AlpDecode<T>::decode_value(encodedValue, floatMetadata->fac, floatMetadata->exp);
        if (floatValue != decodedValue) {
            ++newExceptionCount;
            encodedValues[i] = bitpackingInfo.offset;
        } else {
            encodedValues[i] = encodedValue;
        }
    }
    localUpdateState.floatState.newExceptionCount += newExceptionCount;
    const size_t totalExceptionCount =
        floatMetadata->exceptionCount + localUpdateState.floatState.newExceptionCount;
    const bool exceptionsOK = totalExceptionCount <= floatMetadata->exceptionCapacity;

    return exceptionsOK &&
           metadata.getChild(BITPACKING_CHILD_IDX)
               .canUpdateInPlace(reinterpret_cast<uint8_t*>(encodedValues.data()), 0,
                   encodedValues.size(), getBitpackingLogicalType<T>().getPhysicalType(),
                   localUpdateState);
}

template<std::floating_point T>
common::page_idx_t FloatCompression<T>::getNumDataPages(common::page_idx_t numTotalPages,
    const CompressionMetadata& compMeta) {
    return numTotalPages -
           EncodeException<T>::numPagesFromExceptions(compMeta.floatMetadata()->exceptionCapacity);
}

template<std::floating_point T>
uint64_t FloatCompression<T>::getMaxCapacity(common::page_idx_t numPages,
    CompressionMetadata compMeta) {
    return IntegerBitpacking<EncodedType>::getMaxCapacity(numPages, compMeta);
}

template class FloatCompression<double>;
template class FloatCompression<float>;

template struct EncodeException<double>;
template struct EncodeException<float>;

template struct EncodeExceptionView<double>;
template struct EncodeExceptionView<float>;

} // namespace storage
} // namespace kuzu
