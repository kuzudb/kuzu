#pragma once

#include <cmath>

#include "common/exception/overflow.h"
#include "common/string_format.h"
#include "common/type_utils.h"
#include "common/vector/value_vector.h"
#include "function/cast/functions/numeric_cast.h"

namespace kuzu {
namespace function {

struct CastToString {
    template<typename T>
    static inline void operation(T& input, common::ku_string_t& result,
        common::ValueVector& inputVector, common::ValueVector& resultVector) {
        std::string resultStr = common::TypeUtils::toString(input, (void*)&inputVector);
        if (resultStr.length() > common::ku_string_t::SHORT_STR_LENGTH) {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                common::StringVector::getInMemOverflowBuffer(&resultVector)
                    ->allocateSpace(resultStr.length()));
        }
        result.set(resultStr);
    }
};

struct CastDateToTimestamp {
    static inline void operation(common::date_t& input, common::timestamp_t& result) {
        result = common::Timestamp::fromDateTime(input, common::dtime_t{});
    }
};

struct CastToDouble {
    template<typename T>
    static inline void operation(T& input, double_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within DOUBLE range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToDouble::operation(common::int128_t& input, double_t& result) {
    if (!common::Int128_t::tryCast(input, result)) { // LCOV_EXCL_START
        throw common::OverflowException{common::stringFormat(
            "Value {} is not within DOUBLE range", common::TypeUtils::toString(input))};
    } // LCOV_EXCL_STOP
}

struct CastToFloat {
    template<typename T>
    static inline void operation(T& input, float_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within FLOAT range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToFloat::operation(common::int128_t& input, float_t& result) {
    if (!common::Int128_t::tryCast(input, result)) { // LCOV_EXCL_START
        throw common::OverflowException{common::stringFormat(
            "Value {} is not within FLOAT range", common::TypeUtils::toString(input))};
    }; // LCOV_EXCL_STOP
}

struct CastToInt128 {
    template<typename T>
    static inline void operation(T& input, common::int128_t& result) {
        common::Int128_t::tryCastTo(input, result);
    }
};

struct CastToInt64 {
    template<typename T>
    static inline void operation(T& input, int64_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within INT64 range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToInt64::operation(common::int128_t& input, int64_t& result) {
    if (!common::Int128_t::tryCast(input, result)) {
        throw common::OverflowException{common::stringFormat(
            "Value {} is not within INT64 range", common::TypeUtils::toString(input))};
    };
}

struct CastToSerial {
    template<typename T>
    static inline void operation(T& input, int64_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within INT64 range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToSerial::operation(common::int128_t& input, int64_t& result) {
    if (!common::Int128_t::tryCast(input, result)) {
        throw common::OverflowException{common::stringFormat(
            "Value {} is not within INT64 range", common::TypeUtils::toString(input))};
    };
}

struct CastToInt32 {
    template<typename T>
    static inline void operation(T& input, int32_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within INT32 range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToInt32::operation(common::int128_t& input, int32_t& result) {
    if (!common::Int128_t::tryCast(input, result)) {
        throw common::OverflowException{common::stringFormat(
            "Value {} is not within INT32 range", common::TypeUtils::toString(input))};
    };
}

struct CastToInt16 {
    template<typename T>
    static inline void operation(T& input, int16_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within INT16 range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToInt16::operation(common::int128_t& input, int16_t& result) {
    if (!common::Int128_t::tryCast(input, result)) {
        throw common::OverflowException{common::stringFormat(
            "Value {} is not within INT16 range", common::TypeUtils::toString(input))};
    };
}

struct CastToInt8 {
    template<typename T>
    static inline void operation(T& input, int8_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within INT8 range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToInt8::operation(common::int128_t& input, int8_t& result) {
    if (!common::Int128_t::tryCast(input, result)) {
        throw common::OverflowException{common::stringFormat(
            "Value {} is not within INT8 range", common::TypeUtils::toString(input))};
    };
}

struct CastToUInt64 {
    template<typename T>
    static inline void operation(T& input, uint64_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within UINT64 range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToUInt64::operation(common::int128_t& input, uint64_t& result) {
    if (!common::Int128_t::tryCast(input, result)) {
        throw common::OverflowException{common::stringFormat(
            "Value {} is not within UINT64 range", common::TypeUtils::toString(input))};
    };
}

struct CastToUInt32 {
    template<typename T>
    static inline void operation(T& input, uint32_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within UINT32 range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToUInt32::operation(common::int128_t& input, uint32_t& result) {
    if (!common::Int128_t::tryCast(input, result)) {
        throw common::OverflowException{common::stringFormat(
            "Value {} is not within UINT32 range", common::TypeUtils::toString(input))};
    };
}

struct CastToUInt16 {
    template<typename T>
    static inline void operation(T& input, uint16_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within UINT16 range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToUInt16::operation(common::int128_t& input, uint16_t& result) {
    if (!common::Int128_t::tryCast(input, result)) {
        throw common::OverflowException{common::stringFormat(
            "Value {} is not within UINT16 range", common::TypeUtils::toString(input))};
    };
}

struct CastToUInt8 {
    template<typename T>
    static inline void operation(T& input, uint8_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within UINT8 range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToUInt8::operation(common::int128_t& input, uint8_t& result) {
    if (!common::Int128_t::tryCast(input, result)) {
        throw common::OverflowException{common::stringFormat(
            "Value {} is not within UINT8 range", common::TypeUtils::toString(input))};
    };
}

} // namespace function
} // namespace kuzu
