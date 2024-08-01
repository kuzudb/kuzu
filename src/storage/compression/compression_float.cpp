#include "storage/compression/compression_float.h"

#include "alp/encode.hpp"
#include "common/constants.h"
#include "common/utils.h"
#include <ranges>

namespace kuzu {
namespace storage {

namespace {
static constexpr common::idx_t BITPACKING_CHILD_IDX = 0;
}

template<std::floating_point T>
size_t EncodeException<T>::numPagesFromExceptions(size_t exceptionCount) {
    return common::ceilDiv(static_cast<uint64_t>(exceptionCount),
        common::BufferPoolConstants::PAGE_4KB_SIZE / sizeBytes());
}

template<std::floating_point T>
size_t EncodeException<T>::exceptionBytesPerPage() {
    return common::BufferPoolConstants::PAGE_4KB_SIZE / sizeBytes() * sizeBytes();
}

template<std::floating_point T>
bool EncodeException<T>::operator<(const EncodeException<T>& o) const {
    KU_ASSERT(posInChunk != o.posInChunk);
    return posInChunk < o.posInChunk;
}

template<std::floating_point T>
EncodeException<T> EncodeExceptionView<T>::getValue() const {
    EncodeException<T> ret;
    std::memcpy(&ret.value, bytes, sizeof(ret.value));
    std::memcpy(&ret.posInChunk, bytes + sizeof(ret.value), sizeof(ret.posInChunk));
    return ret;
}

template<std::floating_point T>
void EncodeExceptionView<T>::setValue(EncodeException<T> exception) {
    std::memcpy(bytes, &exception.value, sizeof(exception.value));
    std::memcpy(bytes + sizeof(exception.value), &exception.posInChunk,
        sizeof(exception.posInChunk));
}

template<std::floating_point T>
uint64_t FloatCompression<T>::compressNextPage(const uint8_t*&, uint64_t, uint8_t*, uint64_t,
    const struct CompressionMetadata&) const {
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

    std::vector<EncodedType> integerEncodedValues(numValuesToCompress);
    for (size_t posInPage = 0; posInPage < numValuesToCompress; ++posInPage) {
        const auto floatValue = reinterpret_cast<const T*>(srcBuffer)[posInPage];
        const EncodedType encodedValue = alp::AlpEncode<T>::encode_value(floatValue,
            metadata.floatMetadata().fac, metadata.floatMetadata().exp);
        const double decodedValue = alp::AlpDecode<T>::decode_value(encodedValue,
            metadata.floatMetadata().fac, metadata.floatMetadata().exp);

        if (floatValue != decodedValue) {
            KU_ASSERT(
                (exceptionCount + 1) * EncodeException<T>::sizeBytes() <= exceptionBufferSize);
            auto* exceptionBufferEntry = reinterpret_cast<std::byte*>(
                exceptionBuffer + exceptionCount * EncodeException<T>::sizeBytes());
            EncodeExceptionView<T>{exceptionBufferEntry}.setValue({.value = floatValue,
                .posInChunk = common::narrowingConversion<uint32_t>(srcOffset + posInPage)});

            // We don't need to replace with 1st successful encode as the integer bitpacking
            // metadata is already populated
            ++exceptionCount;
        } else {
            integerEncodedValues[posInPage] = encodedValue;
        }
    }
    srcBuffer += numValuesToCompress * sizeof(T);

    const auto* castedIntegerEncodedBuffer =
        reinterpret_cast<const uint8_t*>(integerEncodedValues.data());
    const auto compressedIntegerSize =
        encodedFloatBitpacker.compressNextPage(castedIntegerEncodedBuffer, numValuesToCompress,
            dstBuffer, dstBufferSize, metadata.getChild(BITPACKING_CHILD_IDX));

    memset(dstBuffer + compressedIntegerSize, 0, dstBufferSize - compressedIntegerSize);

    // since we already do the zeroing we return the size of the whole page
    return dstBufferSize;
}

template<std::floating_point T>
uint64_t FloatCompression<T>::numValues(uint64_t dataSize, const CompressionMetadata& metadata) {
    return decltype(encodedFloatBitpacker)::numValues(dataSize,
        metadata.getChild(BITPACKING_CHILD_IDX));
}

template<std::floating_point T>
void FloatCompression<T>::decompressFromPage(const uint8_t* srcBuffer, uint64_t srcOffset,
    uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues,
    const struct CompressionMetadata& metadata) const {

    std::vector<EncodedType> integerEncodedValues(numValues);
    encodedFloatBitpacker.decompressFromPage(srcBuffer, srcOffset,
        reinterpret_cast<uint8_t*>(integerEncodedValues.data()), 0, numValues,
        metadata.getChild(BITPACKING_CHILD_IDX));

    for (size_t i = 0; i < numValues; ++i) {
        reinterpret_cast<T*>(dstBuffer)[dstOffset + i] = alp::AlpDecode<T>::decode_value(
            integerEncodedValues[i], metadata.floatMetadata().fac, metadata.floatMetadata().exp);
    }
}

template<std::floating_point T>
void FloatCompression<T>::setValuesFromUncompressed(const uint8_t* srcBuffer,
    common::offset_t srcOffset, uint8_t* dstBuffer, common::offset_t dstOffset,
    common::offset_t numValues, const CompressionMetadata& metadata,
    const common::NullMask* nullMask) const {
    RUNTIME_CHECK(CompressionMetadata::InPlaceUpdateLocalState localUpdateState{});
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
            metadata.floatMetadata().fac, metadata.floatMetadata().exp);
        integerEncodedValues[i] = encodedValue;
    }

    encodedFloatBitpacker.setValuesFromUncompressed(
        reinterpret_cast<const uint8_t*>(integerEncodedValues.data()), 0, dstBuffer, dstOffset,
        numValues, metadata.getChild(BITPACKING_CHILD_IDX), nullMask);
}

template<std::floating_point T>
BitpackInfo<typename FloatCompression<T>::EncodedType> FloatCompression<T>::getBitpackInfo(
    const CompressionMetadata& metadata) {
    return decltype(encodedFloatBitpacker)::getPackingInfo(metadata.getChild(BITPACKING_CHILD_IDX));
}

template<std::floating_point T>
bool FloatCompression<T>::canUpdateInPlace(std::span<const T> value,
    const CompressionMetadata& metadata,
    CompressionMetadata::InPlaceUpdateLocalState& localUpdateState,
    const std::optional<common::NullMask>& nullMask, uint64_t nullMaskOffset) {
    size_t newExceptionCount = 0;
    std::vector<EncodedType> encodedValues(value.size());
    const auto bitpackingInfo =
        decltype(encodedFloatBitpacker)::getPackingInfo(metadata.getChild(BITPACKING_CHILD_IDX));
    for (size_t i = 0; i < value.size(); ++i) {
        if (nullMask && nullMask->isNull(nullMaskOffset + i)) {
            continue;
        }

        const auto floatValue = value[i];
        const EncodedType encodedValue = alp::AlpEncode<T>::encode_value(floatValue,
            metadata.floatMetadata().fac, metadata.floatMetadata().exp);
        const double decodedValue = alp::AlpDecode<T>::decode_value(encodedValue,
            metadata.floatMetadata().fac, metadata.floatMetadata().exp);
        if (floatValue != decodedValue) {
            ++newExceptionCount;
            encodedValues[i] = bitpackingInfo.offset;
        } else {
            encodedValues[i] = encodedValue;
        }
    }
    localUpdateState.floatState.newExceptionCount += newExceptionCount;
    // hack: if we rewrite the entire chunk we don't take into account previous exceptions
    const size_t totalExceptionCount =
        (value.size() == common::StorageConstants::NODE_GROUP_SIZE) ?
            localUpdateState.floatState.newExceptionCount :
            metadata.floatMetadata().exceptionCount + localUpdateState.floatState.newExceptionCount;
    // const size_t totalExceptionCount =
    //     metadata.getFloatMetadata().exceptionCount +
    //     localUpdateState.floatState.newExceptionCount;
    const bool exceptionsOK = totalExceptionCount <= metadata.floatMetadata().exceptionCapacity;

    return exceptionsOK && decltype(encodedFloatBitpacker)::canUpdateInPlace(encodedValues,
                               metadata.getChild(BITPACKING_CHILD_IDX), nullMask, nullMaskOffset);
}

template class FloatCompression<double>;
template class FloatCompression<float>;

template struct EncodeException<double>;
template struct EncodeException<float>;

template struct EncodeExceptionView<double>;
template struct EncodeExceptionView<float>;

} // namespace storage
} // namespace kuzu
