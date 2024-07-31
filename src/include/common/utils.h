#pragma once

#include <cstdint>
#include <limits>
#include <vector>

#include "common/assert.h"
#include "common/numeric_utils.h"
#include "common/types/int128_t.h"
#include <span>

namespace kuzu {
namespace common {

class BitmaskUtils {
public:
    template<typename T>
        requires std::integral<T>
    static T all1sMaskForLeastSignificantBits(uint32_t numBits) {
        KU_ASSERT(numBits <= 64);
        using U = numeric_utils::MakeUnSignedT<T>;
        return (T)(numBits == (sizeof(U) * 8) ? std::numeric_limits<U>::max() :
                                                static_cast<U>(((U)1 << numBits) - 1));
    }

    // constructs all 1s mask while avoiding overflow/underflow for int128
    template<typename T>
        requires std::same_as<std::remove_cvref_t<T>, int128_t>
    static T all1sMaskForLeastSignificantBits(uint32_t numBits) {
        static constexpr uint8_t numBitsInT = sizeof(T) * 8;

        // use ~T(1) instead of ~T(0) to avoid sign-bit filling
        const T fullMask = ~(T(1) << (numBitsInT - 1));

        const size_t numBitsToDiscard = (numBitsInT - 1 - numBits);
        return (fullMask >> numBitsToDiscard);
    }
};

uint64_t nextPowerOfTwo(uint64_t v);
uint64_t prevPowerOfTwo(uint64_t v);

bool isLittleEndian();

template<typename T>
bool integerFitsIn(int64_t val);

template<typename T>
std::vector<T> copyVector(const std::vector<T>& objects) {
    std::vector<T> result;
    result.reserve(objects.size());
    for (auto& object : objects) {
        result.push_back(object->copy());
    }
    return result;
}

template<typename T>
size_t writeValueToVector(std::vector<std::byte>& vec, offset_t byteOffset, const T& val) {
    static_assert(std::is_trivially_copyable_v<T>);
    static constexpr size_t bytesToWrite = sizeof(val);
    memcpy(vec.data() + byteOffset, &val, bytesToWrite);
    return bytesToWrite;
}

template<typename T>
size_t readValueFromSpan(const std::span<const std::byte>& vec, offset_t byteOffset, T& val) {
    static constexpr size_t bytesToRead = sizeof(val);
    memcpy(&val, vec.data() + byteOffset, bytesToRead);
    return bytesToRead;
}

template<numeric_utils::IsIntegral T>
constexpr T ceilDiv(T a, T b) {
    return (a / b) + (a % b != 0);
}

template<std::integral To, std::integral From>
constexpr To narrowingConversion(From val) {
    KU_ASSERT(static_cast<To>(val) == val);
    return val;
}

} // namespace common
} // namespace kuzu
