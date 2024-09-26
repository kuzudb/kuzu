#pragma once

#include <cstdint>
#include <limits>

#include "common/assert.h"
#include "common/numeric_utils.h"
#include "common/types/int128_t.h"

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

template<numeric_utils::IsIntegral T>
constexpr T ceilDiv(T a, T b) {
    return (a / b) + (a % b != 0);
}

template<std::integral To, std::integral From>
constexpr To safeIntegerConversion(From val) {
    KU_ASSERT(static_cast<To>(val) == val);
    return val;
}

} // namespace common
} // namespace kuzu
