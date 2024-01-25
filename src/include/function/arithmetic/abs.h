#pragma once

#include <cstdint>

#include "common/types/int128_t.h"

namespace kuzu {
namespace function {

struct Abs {
    template<class T>
    static inline void operation(T& input, T& result) {
        if constexpr (std::is_unsigned_v<T>) {
            result = input;
        } else {
            result = std::abs(input);
        }
    }
};

template<>
void Abs::operation(int8_t& input, int8_t& result);

template<>
void Abs::operation(int16_t& input, int16_t& result);

template<>
void Abs::operation(int32_t& input, int32_t& result);

template<>
void Abs::operation(int64_t& input, int64_t& result);

template<>
void Abs::operation(common::int128_t& input, common::int128_t& result);

} // namespace function
} // namespace kuzu
