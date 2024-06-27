#include "storage/compression/compression_float.h"

#include "alp/encode.hpp"

namespace kuzu {
namespace storage {
template<std::floating_point T>
uint64_t FloatCompression<T>::compressNextPage(const uint8_t*& srcBuffer,
    uint64_t numValuesRemaining, uint8_t* dstBuffer, uint64_t dstBufferSize,
    const struct CompressionMetadata& metadata) const {
    const size_t numValuesToCompress =
        std::min(numValuesRemaining, numValues(dstBufferSize, metadata));

    std::vector<int64_t> integerEncodedValues(numValuesToCompress);
    for (size_t i = 0; i < numValuesToCompress; ++i) {
        const auto floatValue = std::bit_cast<T*>(srcBuffer)[i];
        integerEncodedValues[i] = alp::AlpEncode<T>::encode_value(floatValue,
            metadata.floatMetadata.fac, metadata.floatMetadata.exp);
    }
    srcBuffer += numValuesToCompress;

    const auto* castedSrcBuffer = reinterpret_cast<const uint8_t*>(integerEncodedValues.data());
    return IntegerBitpacking<int64_t>().compressNextPage(castedSrcBuffer, numValuesToCompress,
        dstBuffer, dstBufferSize, metadata);
}

template<std::floating_point T>
void FloatCompression<T>::decompressFromPage(const uint8_t* srcBuffer, uint64_t srcOffset,
    uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues,
    const struct CompressionMetadata& metadata) const {}

} // namespace storage
} // namespace kuzu
