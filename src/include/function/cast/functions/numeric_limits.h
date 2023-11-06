#pragma once

#include <cstdint>
#include <limits>

#include "common/types/int128_t.h"

namespace kuzu {
namespace function {

template<class T>
struct NumericLimits {
    static constexpr T minimum() { return std::numeric_limits<T>::lowest(); }
    static constexpr T maximum() { return std::numeric_limits<T>::max(); }
    static constexpr bool isSigned() { return std::is_signed<T>::value; }
    static constexpr uint64_t digits();
};

template<>
struct NumericLimits<common::int128_t> {
    static constexpr common::int128_t minimum() {
        return {static_cast<uint64_t>(std::numeric_limits<int64_t>::lowest()), 1};
    }
    static constexpr common::int128_t maximum() {
        return {std::numeric_limits<int64_t>::max(),
            static_cast<int64_t>(std::numeric_limits<uint64_t>::max())};
    }
    static constexpr bool isSigned() { return true; }
    static constexpr uint64_t digits() { return 39; }
};

template<>
constexpr uint64_t NumericLimits<int8_t>::digits() {
    return 3;
}

template<>
constexpr uint64_t NumericLimits<int16_t>::digits() {
    return 5;
}

template<>
constexpr uint64_t NumericLimits<int32_t>::digits() {
    return 10;
}

template<>
constexpr uint64_t NumericLimits<int64_t>::digits() {
    return 19;
}

template<>
constexpr uint64_t NumericLimits<uint8_t>::digits() {
    return 3;
}

template<>
constexpr uint64_t NumericLimits<uint16_t>::digits() {
    return 5;
}

template<>
constexpr uint64_t NumericLimits<uint32_t>::digits() {
    return 10;
}

template<>
constexpr uint64_t NumericLimits<uint64_t>::digits() {
    return 20;
}

template<>
constexpr uint64_t NumericLimits<float>::digits() {
    return 127;
}

template<>
constexpr uint64_t NumericLimits<double>::digits() {
    return 250;
}

} // namespace function
} // namespace kuzu
