#pragma once

#include <cassert>

#include "common/exception/conversion.h"
#include "common/string_utils.h"
#include "common/type_utils.h"
#include "common/types/ku_string.h"
#include "common/vector/value_vector.h"
#include "fast_float.h"
#include "numeric_limits.h"

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
    // TODO: handle decimals

    template<class T, bool NEGATIVE>
    static bool finalize(T& state) {
        return true;
    }
};

// cast string to bool
bool tryCastToBool(const char* input, uint64_t len, bool& result);
void castStringToBool(const char* input, uint64_t len, bool& result);

// cast to numerical values
// TODO: support exponent + decimal
template<class T, bool NEGATIVE, bool ALLOW_EXPONENT = false, class OP = IntegerCastOperation>
static bool integerCastLoop(const char* input, uint64_t len, T& result) {
    int64_t start_pos = 0;
    if (NEGATIVE) {
        start_pos = 1;
    }
    int64_t pos = start_pos;
    while (pos < len) {
        if (!common::StringUtils::CharacterIsDigit(input[pos])) {
            // TODO: exponent and decimals
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

template<typename T, bool IS_SIGNED = true>
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
        return integerCastLoop<T, true>(input, len, result);
    }

    // not allow leading 0
    if (len > 1 && *input == '0') {
        return false;
    }

    return integerCastLoop<T, false>(input, len, result);
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
        throw common::ConversionException(
            "Cast failed. " + std::string{input, len} + " is not in " +
            common::LogicalTypeUtils::dataTypeToString(type) + " range.");
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
        throw common::ConversionException(
            "Cast failed. " + std::string{input, len} + " is not in " +
            common::LogicalTypeUtils::dataTypeToString(type) + " range.");
    }
}

template<typename T>
static inline T castStringToNum(const char* input, uint64_t len,
    const common::LogicalType& type = common::LogicalType{common::LogicalTypeID::ANY}) {
    T result;
    simpleIntegerCast(input, len, result, type);
    return result;
}

template<>
inline uint64_t castStringToNum(const char* input, uint64_t len, const common::LogicalType& type) {
    uint64_t result;
    simpleIntegerCast<uint64_t, false>(input, len, result, type);
    return result;
}

template<>
uint32_t castStringToNum(const char* input, uint64_t len, const common::LogicalType& type) {
    uint32_t result;
    simpleIntegerCast<uint32_t, false>(input, len, result, type);
    return result;
}

template<>
uint16_t castStringToNum(const char* input, uint64_t len, const common::LogicalType& type) {
    uint16_t result;
    simpleIntegerCast<uint16_t, false>(input, len, result, type);
    return result;
}

template<>
uint8_t castStringToNum(const char* input, uint64_t len, const common::LogicalType& type) {
    uint8_t result;
    simpleIntegerCast<uint8_t, false>(input, len, result, type);
    return result;
}

template<>
inline double_t castStringToNum(const char* input, uint64_t len, const common::LogicalType& type) {
    double_t result;
    doubleCast<double_t>(input, len, result, type);
    return result;
}

template<>
inline float_t castStringToNum(const char* input, uint64_t len, const common::LogicalType& type) {
    float_t result;
    doubleCast<float_t>(input, len, result, type);
    return result;
}

template<class SRC, class DST>
static bool tryCastWithOverflowCheck(SRC value, DST& result) {
    if (NumericLimits<SRC>::isSigned() != NumericLimits<DST>::isSigned()) {
        if (NumericLimits<SRC>::isSigned()) {
            if (NumericLimits<SRC>::digits() > NumericLimits<DST>::digits()) {
                if (value < 0 || value > (SRC)NumericLimits<DST>::maximum()) {
                    return false;
                }
            } else {
                if (value < 0) {
                    return false;
                }
            }
            result = (DST)value;
            return true;
        } else {
            // unsigned to signed conversion
            if (NumericLimits<SRC>::digits() >= NumericLimits<DST>::digits()) {
                if (value <= (SRC)NumericLimits<DST>::maximum()) {
                    result = (DST)value;
                    return true;
                }
                return false;
            } else {
                result = (DST)value;
                return true;
            }
        }
    } else {
        // same sign conversion
        if (NumericLimits<DST>::digits() >= NumericLimits<SRC>::digits()) {
            result = (DST)value;
            return true;
        } else {
            if (value < SRC(NumericLimits<DST>::minimum()) ||
                value > SRC(NumericLimits<DST>::maximum())) {
                return false;
            }
            result = (DST)value;
            return true;
        }
    }
}

template<class SRC, class T>
bool tryCastWithOverflowCheckFloat(SRC value, T& result, SRC min, SRC max) {
    if (!(value >= min && value < max)) {
        return false;
    }
    // PG FLOAT => INT casts use statistical rounding.
    result = std::nearbyint(value);
    return true;
}

template<>
bool tryCastWithOverflowCheck(float value, int8_t& result) {
    return tryCastWithOverflowCheckFloat<float, int8_t>(value, result, -128.0f, 128.0f);
}

template<>
bool tryCastWithOverflowCheck(float value, int16_t& result) {
    return tryCastWithOverflowCheckFloat<float, int16_t>(value, result, -32768.0f, 32768.0f);
}

template<>
bool tryCastWithOverflowCheck(float value, int32_t& result) {
    return tryCastWithOverflowCheckFloat<float, int32_t>(
        value, result, -2147483648.0f, 2147483648.0f);
}

template<>
bool tryCastWithOverflowCheck(float value, int64_t& result) {
    return tryCastWithOverflowCheckFloat<float, int64_t>(
        value, result, -9223372036854775808.0f, 9223372036854775808.0f);
}

template<>
bool tryCastWithOverflowCheck(float value, uint8_t& result) {
    return tryCastWithOverflowCheckFloat<float, uint8_t>(value, result, 0.0f, 256.0f);
}

template<>
bool tryCastWithOverflowCheck(float value, uint16_t& result) {
    return tryCastWithOverflowCheckFloat<float, uint16_t>(value, result, 0.0f, 65536.0f);
}

template<>
bool tryCastWithOverflowCheck(float value, uint32_t& result) {
    return tryCastWithOverflowCheckFloat<float, uint32_t>(value, result, 0.0f, 4294967296.0f);
}

template<>
bool tryCastWithOverflowCheck(float value, uint64_t& result) {
    return tryCastWithOverflowCheckFloat<float, uint64_t>(
        value, result, 0.0f, 18446744073709551616.0f);
}

template<>
bool tryCastWithOverflowCheck(double value, int8_t& result) {
    return tryCastWithOverflowCheckFloat<double, int8_t>(value, result, -128.0, 128.0);
}

template<>
bool tryCastWithOverflowCheck(double value, int16_t& result) {
    return tryCastWithOverflowCheckFloat<double, int16_t>(value, result, -32768.0, 32768.0);
}

template<>
bool tryCastWithOverflowCheck(double value, int32_t& result) {
    return tryCastWithOverflowCheckFloat<double, int32_t>(
        value, result, -2147483648.0, 2147483648.0);
}

template<>
bool tryCastWithOverflowCheck(double value, int64_t& result) {
    return tryCastWithOverflowCheckFloat<double, int64_t>(
        value, result, -9223372036854775808.0, 9223372036854775808.0);
}

template<>
bool tryCastWithOverflowCheck(double value, uint8_t& result) {
    return tryCastWithOverflowCheckFloat<double, uint8_t>(value, result, 0.0, 256.0);
}

template<>
bool tryCastWithOverflowCheck(double value, uint16_t& result) {
    return tryCastWithOverflowCheckFloat<double, uint16_t>(value, result, 0.0, 65536.0);
}

template<>
bool tryCastWithOverflowCheck(double value, uint32_t& result) {
    return tryCastWithOverflowCheckFloat<double, uint32_t>(value, result, 0.0, 4294967296.0);
}

template<>
bool tryCastWithOverflowCheck(double value, uint64_t& result) {
    return tryCastWithOverflowCheckFloat<double, uint64_t>(
        value, result, 0.0, 18446744073709551615.0);
}

template<>
bool tryCastWithOverflowCheck(float input, double& result) {
    result = double(input);
    return true;
}

template<>
bool tryCastWithOverflowCheck(double input, float& result) {
    result = float(input);
    return true;
}

} // namespace function
} // namespace kuzu
