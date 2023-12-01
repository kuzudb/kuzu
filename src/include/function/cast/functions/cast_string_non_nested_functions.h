#pragma once

#include "common/exception/conversion.h"
#include "common/string_utils.h"
#include "common/types/int128_t.h"
#include "common/types/types.h"
#include "fast_float.h"
#include "function/cast/functions/numeric_limits.h"

namespace kuzu {
namespace function {

// cast string to numerical
template<typename T>
struct IntegerCastData {
    using Result = T;
    Result result;
};

struct IntegerCastOperation {
    template<class T, bool NEGATIVE>
    static bool handleDigit(T& state, uint8_t digit) {
        using result_t = typename T::Result;
        if constexpr (NEGATIVE) {
            if (state.result < ((std::numeric_limits<result_t>::min() + digit) / 10)) {
                return false;
            }
            state.result = state.result * 10 - digit;
        } else {
            if (state.result > ((std::numeric_limits<result_t>::max() - digit) / 10)) {
                return false;
            }
            state.result = state.result * 10 + digit;
        }
        return true;
    }

    // TODO(Kebing): handle decimals
    template<class T, bool NEGATIVE>
    static bool finalize(T& /*state*/) {
        return true;
    }
};

// cast string to bool
bool tryCastToBool(const char* input, uint64_t len, bool& result);
void castStringToBool(const char* input, uint64_t len, bool& result);

// cast to numerical values
// TODO(Kebing): support exponent + decimal
template<typename T, bool NEGATIVE, bool ALLOW_EXPONENT = false, class OP>
static bool integerCastLoop(const char* input, uint64_t len, T& result) {
    int64_t start_pos = 0;
    if (NEGATIVE) {
        start_pos = 1;
    }
    int64_t pos = start_pos;
    while (pos < len) {
        if (!common::StringUtils::CharacterIsDigit(input[pos])) {
            return false;
        }
        uint8_t digit = input[pos++] - '0';
        if (!OP::template handleDigit<T, NEGATIVE>(result, digit)) {
            return false;
        }
    } // append all digits to result
    if (!OP::template finalize<T, NEGATIVE>(result)) {
        return false;
    }
    return pos > start_pos; // false if no digits "" or "-"
}

template<typename T, bool IS_SIGNED = true, class OP = IntegerCastOperation>
static bool tryIntegerCast(const char* input, uint64_t& len, T& result) {
    common::StringUtils::removeCStringWhiteSpaces(input, len);
    if (len == 0) {
        return false;
    }

    // negative
    if (*input == '-') {
        if constexpr (!IS_SIGNED) { // unsigned if not -0
            uint64_t pos = 1;
            while (pos < len) {
                if (input[pos++] != '0') {
                    return false;
                }
            }
        }
        // decimal separator is default to "."
        return integerCastLoop<T, true, false, OP>(input, len, result);
    }

    // not allow leading 0
    if (len > 1 && *input == '0') {
        return false;
    }
    return integerCastLoop<T, false, false, OP>(input, len, result);
}

struct Int128CastData {
    common::int128_t result;
    int64_t intermediate;
    uint8_t digits;
    bool decimal;

    bool flush() {
        if (digits == 0 && intermediate == 0) {
            return true;
        }
        if (result.low != 0 || result.high != 0) {
            if (digits > 38) {
                return false;
            }
            if (!common::Int128_t::tryMultiply(
                    result, common::Int128_t::powerOf10[digits], result)) {
                return false;
            }
        }
        if (!common::Int128_t::addInPlace(result, common::int128_t(intermediate))) {
            return false;
        }
        digits = 0;
        intermediate = 0;
        return true;
    }
};

struct Int128CastOperation {
    template<typename T, bool NEGATIVE>
    static bool handleDigit(T& result, uint8_t digit) {
        if (NEGATIVE) {
            if (result.intermediate < (NumericLimits<int64_t>::minimum() + digit) / 10) {
                if (!result.flush()) {
                    return false;
                }
            }
            result.intermediate *= 10;
            result.intermediate -= digit;
        } else {
            if (result.intermediate > (std::numeric_limits<int64_t>::max() - digit) / 10) {
                if (!result.flush()) {
                    return false;
                }
            }
            result.intermediate *= 10;
            result.intermediate += digit;
        }
        result.digits++;
        return true;
    }

    template<typename T, bool NEGATIVE>
    static bool finalize(T& result) {
        return result.flush();
    }
};

static bool trySimpleInt128Cast(const char* input, uint64_t len, common::int128_t& result) {
    Int128CastData data{};
    data.result = 0;
    if (tryIntegerCast<Int128CastData, true, Int128CastOperation>(input, len, data)) {
        result = data.result;
        return true;
    }
    return false;
}

static void simpleInt128Cast(const char* input, uint64_t len, common::int128_t& result) {
    if (!trySimpleInt128Cast(input, len, result)) {
        throw common::ConversionException(
            "Cast failed. " + std::string{input, len} + " is not within INT128 range.");
    }
}

template<typename T, bool IS_SIGNED = true>
static bool trySimpleIntegerCast(const char* input, uint64_t len, T& result) {
    IntegerCastData<T> data;
    data.result = 0;
    if (tryIntegerCast<IntegerCastData<T>, IS_SIGNED>(input, len, data)) {
        result = data.result;
        return true;
    }
    return false;
}

template<class T, bool IS_SIGNED = true>
static void simpleIntegerCast(const char* input, uint64_t len, T& result,
    const common::LogicalType& type = common::LogicalType{common::LogicalTypeID::ANY}) {
    if (!trySimpleIntegerCast<T, IS_SIGNED>(input, len, result)) {
        throw common::ConversionException("Cast failed. " + std::string{input, len} +
                                          " is not in " + type.toString() + " range.");
    }
}

template<class T>
static bool tryDoubleCast(const char* input, uint64_t len, T& result) {
    common::StringUtils::removeCStringWhiteSpaces(input, len);
    if (len == 0) {
        return false;
    }
    // not allow leading 0
    if (len > 1 && *input == '0') {
        if (common::StringUtils::CharacterIsDigit(input[1])) {
            return false;
        }
    }
    auto end = input + len;
    auto parse_result = kuzu_fast_float::from_chars(input, end, result);
    if (parse_result.ec != std::errc()) {
        return false;
    }
    return parse_result.ptr == end;
}

template<class T>
static void doubleCast(const char* input, uint64_t len, T& result,
    const common::LogicalType& type = common::LogicalType{common::LogicalTypeID::ANY}) {
    if (!tryDoubleCast<T>(input, len, result)) {
        throw common::ConversionException("Cast failed. " + std::string{input, len} +
                                          " is not in " + type.toString() + " range.");
    }
}

} // namespace function
} // namespace kuzu
