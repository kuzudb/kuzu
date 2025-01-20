#pragma once

#include <algorithm>
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

template<typename T, typename Container>
bool containsValue(const Container& container, const T& value) {
    return std::find(container.begin(), container.end(), value) != container.end();
}

template<class T>
struct CountZeros {};

template<>
struct CountZeros<uint64_t> {
    // see here: https://en.wikipedia.org/wiki/De_Bruijn_sequence
    static idx_t Leading(const uint64_t value_in) {
        if (!value_in) {
            return 64;
        }

        uint64_t value = value_in;

        constexpr uint64_t index64msb[] = {0, 47, 1, 56, 48, 27, 2, 60, 57, 49, 41, 37, 28, 16, 3,
            61, 54, 58, 35, 52, 50, 42, 21, 44, 38, 32, 29, 23, 17, 11, 4, 62, 46, 55, 26, 59, 40,
            36, 15, 53, 34, 51, 20, 43, 31, 22, 10, 45, 25, 39, 14, 33, 19, 30, 9, 24, 13, 18, 8,
            12, 7, 6, 5, 63};

        constexpr uint64_t debruijn64msb = 0X03F79D71B4CB0A89;

        value |= value >> 1;
        value |= value >> 2;
        value |= value >> 4;
        value |= value >> 8;
        value |= value >> 16;
        value |= value >> 32;
        auto result = 63 - index64msb[(value * debruijn64msb) >> 58];
#ifdef __clang__
        if constexpr (sizeof(unsigned long) == sizeof(uint64_t)) {
            KU_ASSERT(result == static_cast<uint64_t>(__builtin_clzl(value_in)));
        }
#endif
        return result;
    }
    static idx_t Trailing(uint64_t value_in) {
        if (!value_in) {
            return 64;
        }
        uint64_t value = value_in;

        constexpr uint64_t index64lsb[] = {63, 0, 58, 1, 59, 47, 53, 2, 60, 39, 48, 27, 54, 33, 42,
            3, 61, 51, 37, 40, 49, 18, 28, 20, 55, 30, 34, 11, 43, 14, 22, 4, 62, 57, 46, 52, 38,
            26, 32, 41, 50, 36, 17, 19, 29, 10, 13, 21, 56, 45, 25, 31, 35, 16, 9, 12, 44, 24, 15,
            8, 23, 7, 6, 5};
        constexpr uint64_t debruijn64lsb = 0x07EDD5E59A4E28C2ULL;
        auto result = index64lsb[((value & -value) * debruijn64lsb) >> 58];
#ifdef __clang__
        if constexpr (sizeof(unsigned long) == sizeof(uint64_t)) {
            KU_ASSERT(result == static_cast<uint64_t>(__builtin_ctzl(value_in)));
        }
#endif
        return result;
    }
};

template<>
struct CountZeros<uint32_t> {
    static idx_t Leading(uint32_t value) { return CountZeros<uint64_t>::Leading(value) - 32; }
    static idx_t Trailing(uint32_t value) { return CountZeros<uint64_t>::Trailing(value); }
};

template<>
struct CountZeros<int128_t> {
    static idx_t Leading(int128_t value) {
        const uint64_t upper = static_cast<uint64_t>(value.high);
        const uint64_t lower = value.low;

        if (upper) {
            return CountZeros<uint64_t>::Leading(upper);
        }
        if (lower) {
            return 64 + CountZeros<uint64_t>::Leading(lower);
        }
        return 128;
    }

    static idx_t Trailing(int128_t value) {
        const uint64_t upper = static_cast<uint64_t>(value.high);
        const uint64_t lower = value.low;

        if (lower) {
            return CountZeros<uint64_t>::Trailing(lower);
        }
        if (upper) {
            return 64 + CountZeros<uint64_t>::Trailing(upper);
        }
        return 128;
    }
};

} // namespace common
} // namespace kuzu
