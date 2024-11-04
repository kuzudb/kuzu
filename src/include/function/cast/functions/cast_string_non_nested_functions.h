#pragma once

#include "common/constants.h"
#include "common/exception/conversion.h"
#include "common/string_format.h"
#include "common/string_utils.h"
#include "common/types/int128_t.h"
#include "common/types/timestamp_t.h"
#include "common/types/types.h"
#include "fast_float.h"
#include "function/cast/functions/numeric_limits.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

bool isAnyType(std::string_view cpy);

LogicalType KUZU_API inferMinimalTypeFromString(const std::string& str);
LogicalType KUZU_API inferMinimalTypeFromString(std::string_view str);
// Infer the type that the string represents.
// Note: minimal integer width is int64
// Used for sniffing

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
void KUZU_API castStringToBool(const char* input, uint64_t len, bool& result);

// cast to numerical values
// TODO(Kebing): support exponent + decimal
template<typename T, bool NEGATIVE, bool ALLOW_EXPONENT = false, class OP>
inline bool integerCastLoop(const char* input, uint64_t len, T& result) {
    auto start_pos = 0u;
    if (NEGATIVE) {
        start_pos = 1;
    }
    auto pos = start_pos;
    while (pos < len) {
        if (!StringUtils::CharacterIsDigit(input[pos])) {
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
inline bool tryIntegerCast(const char* input, uint64_t& len, T& result) {
    StringUtils::removeCStringWhiteSpaces(input, len);
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
    int128_t result = 0;
    int64_t intermediate = 0;
    uint8_t digits = 0;
    bool decimal = false;

    bool flush() {
        if (digits == 0 && intermediate == 0) {
            return true;
        }
        if (result.low != 0 || result.high != 0) {
            if (digits > DECIMAL_PRECISION_LIMIT) {
                return false;
            }
            if (!Int128_t::tryMultiply(result, Int128_t::powerOf10[digits], result)) {
                return false;
            }
        }
        if (!Int128_t::addInPlace(result, int128_t(intermediate))) {
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

inline bool trySimpleInt128Cast(const char* input, uint64_t len, int128_t& result) {
    Int128CastData data{};
    data.result = 0;
    if (tryIntegerCast<Int128CastData, true, Int128CastOperation>(input, len, data)) {
        result = data.result;
        return true;
    }
    return false;
}

inline void simpleInt128Cast(const char* input, uint64_t len, int128_t& result) {
    if (!trySimpleInt128Cast(input, len, result)) {
        throw ConversionException(stringFormat("Cast failed. {} is not within INT128 range.",
            std::string{input, (size_t)len}));
    }
}

template<typename T, bool IS_SIGNED = true>
KUZU_API inline bool trySimpleIntegerCast(const char* input, uint64_t len, T& result) {
    IntegerCastData<T> data{};
    data.result = 0;
    if (tryIntegerCast<IntegerCastData<T>, IS_SIGNED>(input, len, data)) {
        result = data.result;
        return true;
    }
    return false;
}

template<class T, bool IS_SIGNED = true>
KUZU_API inline void simpleIntegerCast(const char* input, uint64_t len, T& result,
    LogicalTypeID typeID = LogicalTypeID::ANY) {
    if (!trySimpleIntegerCast<T, IS_SIGNED>(input, len, result)) {
        throw ConversionException(stringFormat("Cast failed. Could not convert \"{}\" to {}.",
            std::string{input, (size_t)len}, LogicalTypeUtils::toString(typeID)));
    }
}

template<class T>
inline bool tryDoubleCast(const char* input, uint64_t len, T& result) {
    StringUtils::removeCStringWhiteSpaces(input, len);
    if (len == 0) {
        return false;
    }
    // not allow leading 0
    if (len > 1 && *input == '0') {
        if (StringUtils::CharacterIsDigit(input[1])) {
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
inline void doubleCast(const char* input, uint64_t len, T& result,
    LogicalTypeID typeID = LogicalTypeID::ANY) {
    if (!tryDoubleCast<T>(input, len, result)) {
        throw ConversionException(stringFormat("Cast failed. {} is not in {} range.",
            std::string{input, (size_t)len}, LogicalTypeUtils::toString(typeID)));
    }
}

// ---------------------- try cast String to Timestamp -------------------- //
struct TryCastStringToTimestamp {
    template<typename T>
    static bool tryCast(const char* input, uint64_t len, timestamp_t& result);

    template<typename T>
    static void cast(const char* input, uint64_t len, timestamp_t& result, LogicalTypeID typeID) {
        if (!tryCast<T>(input, len, result)) {
            throw ConversionException(Timestamp::getTimestampConversionExceptionMsg(input, len,
                LogicalTypeUtils::toString(typeID)));
        }
    }
};

template<>
bool TryCastStringToTimestamp::tryCast<timestamp_ns_t>(const char* input, uint64_t len,
    timestamp_t& result);

template<>
bool TryCastStringToTimestamp::tryCast<timestamp_ms_t>(const char* input, uint64_t len,
    timestamp_t& result);

template<>
bool TryCastStringToTimestamp::tryCast<timestamp_sec_t>(const char* input, uint64_t len,
    timestamp_t& result);

template<>
bool inline TryCastStringToTimestamp::tryCast<timestamp_tz_t>(const char* input, uint64_t len,
    timestamp_t& result) {
    return Timestamp::tryConvertTimestamp(input, len, result);
}

// ---------------------- cast String to Decimal -------------------- //

template<typename T>
bool tryDecimalCast(const char* input, uint64_t len, T& result, uint32_t precision,
    uint32_t scale) {
    constexpr auto pow10s = pow10Sequence<T>();
    using CAST_OP = typename std::conditional<std::is_same<T, int128_t>::value, Int128CastOperation,
        IntegerCastOperation>::type;
    using CAST_DATA = typename std::conditional<std::is_same<T, int128_t>::value, Int128CastData,
        IntegerCastData<T>>::type;
    StringUtils::removeCStringWhiteSpaces(input, len);
    if (len == 0) {
        return false;
    }

    bool negativeFlag = input[0] == '-';
    if (negativeFlag) {
        input++;
        len -= 1;
    }

    CAST_DATA res;
    res.result = 0;
    auto pos = 0u;
    auto periodPos = len - 1u;
    while (pos < len) {
        auto chr = input[pos];
        if (input[pos] == '.') {
            periodPos = pos;
        } else if (pos > periodPos && pos - periodPos > scale) {
            // we've parsed the digit limit
            break;
        } else if (!StringUtils::CharacterIsDigit(chr) ||
                   !CAST_OP::template handleDigit<CAST_DATA, false>(res, chr - '0')) {
            return false;
        }
        pos++;
    }
    if (pos < len) {
        // then we parsed the digit limit, so round the final digit
        if (!StringUtils::CharacterIsDigit(input[pos])) {
            return false;
        }
        if (!CAST_OP::template finalize<CAST_DATA, false>(res)) {
            return false;
        }
        // then determine rounding
        if (input[pos] >= '5') {
            res.result += 1;
        }
    }
    while (pos - periodPos < scale + 1) {
        // trailing 0's
        if (!CAST_OP::template handleDigit<CAST_DATA, false>(res, 0)) {
            return false;
        }
        pos++;
    }
    if (!CAST_OP::template finalize<CAST_DATA, false>(res)) {
        return false;
    }
    if (res.result >= pow10s[precision]) {
        return false;
    }
    result = negativeFlag ? -res.result : res.result;
    return true;
}

template<typename T>
void decimalCast(const char* input, uint64_t len, T& result, const LogicalType& type) {
    if (!tryDecimalCast(input, len, result, DecimalType::getPrecision(type),
            DecimalType::getScale(type))) {
        throw ConversionException(stringFormat("Cast failed. {} is not in {} range.",
            std::string{input, (size_t)len}, type.toString()));
    }
}

} // namespace function
} // namespace kuzu
