#pragma once

#include <cassert>

#include "cast_utils.h"
#include "common/exception/runtime.h"
#include "common/string_format.h"
#include "common/type_utils.h"
#include "common/types/blob.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct CastStringToDate {
    static inline void operation(common::ku_string_t& input, common::date_t& result) {
        result = common::Date::fromCString((const char*)input.getData(), input.len);
    }
};

struct CastStringToTimestamp {
    static inline void operation(common::ku_string_t& input, common::timestamp_t& result) {
        result = common::Timestamp::fromCString((const char*)input.getData(), input.len);
    }
};

struct CastStringToInterval {
    static inline void operation(common::ku_string_t& input, common::interval_t& result) {
        result = common::Interval::fromCString((const char*)input.getData(), input.len);
    }
};

struct CastToString {
    template<typename T>
    static inline std::string castToStringWithVector(
        T& input, const common::ValueVector& inputVector) {
        return common::TypeUtils::toString(input);
    }

    template<typename T>
    static inline void operation(T& input, common::ku_string_t& result,
        common::ValueVector& inputVector, common::ValueVector& resultVector) {
        std::string resultStr = castToStringWithVector(input, inputVector);
        if (resultStr.length() > common::ku_string_t::SHORT_STR_LENGTH) {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                common::StringVector::getInMemOverflowBuffer(&resultVector)
                    ->allocateSpace(resultStr.length()));
        }
        result.set(resultStr);
    }
};

struct CastToBlob {
    static inline void operation(common::ku_string_t& input, common::blob_t& result,
        common::ValueVector& inputVector, common::ValueVector& resultVector) {
        result.value.len = common::Blob::getBlobSize(input);
        if (!common::ku_string_t::isShortString(result.value.len)) {
            auto overflowBuffer = common::StringVector::getInMemOverflowBuffer(&resultVector);
            auto overflowPtr = overflowBuffer->allocateSpace(result.value.len);
            result.value.overflowPtr = reinterpret_cast<int64_t>(overflowPtr);
            common::Blob::fromString(
                reinterpret_cast<const char*>(input.getData()), input.len, overflowPtr);
            memcpy(result.value.prefix, overflowPtr, common::ku_string_t::PREFIX_LENGTH);
        } else {
            common::Blob::fromString(
                reinterpret_cast<const char*>(input.getData()), input.len, result.value.prefix);
        }
    }
};

struct CastDateToTimestamp {
    static inline void operation(common::date_t& input, common::timestamp_t& result) {
        result = common::Timestamp::fromDateTime(input, common::dtime_t{});
    }
};

template<>
inline std::string CastToString::castToStringWithVector(
    common::list_entry_t& input, const common::ValueVector& inputVector) {
    return common::TypeUtils::toString(input, (void*)&inputVector);
}

template<>
inline std::string CastToString::castToStringWithVector(
    common::struct_entry_t& input, const common::ValueVector& inputVector) {
    return common::TypeUtils::toString(input, (void*)&inputVector);
}

struct CastToBool {
    static inline void operation(common::ku_string_t& input, bool& result) {
        if (!tryCastToBool(reinterpret_cast<const char*>(input.getData()), input.len, result)) {
            throw common::ConversionException{
                common::stringFormat("Value {} is not a valid boolean", input.getAsString())};
        }
    }
};

struct CastToDouble {
    template<typename T>
    static inline void operation(T& input, double_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::RuntimeException{common::stringFormat(
                "Value {} is not within DOUBLE range", common::TypeUtils::toString(input).c_str())};
        }
    }
};

template<>
inline void CastToDouble::operation(char*& input, double_t& result) {
    doubleCast<double_t>(
        input, strlen(input), result, common::LogicalType{common::LogicalTypeID::DOUBLE});
}

template<>
inline void CastToDouble::operation(common::ku_string_t& input, double_t& result) {
    doubleCast<double_t>((char*)input.getData(), input.len, result,
        common::LogicalType{common::LogicalTypeID::DOUBLE});
}

struct CastToFloat {
    template<typename T>
    static inline void operation(T& input, float_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::RuntimeException{common::stringFormat(
                "Value {} is not within FLOAT range", common::TypeUtils::toString(input).c_str())};
        }
    }
};

template<>
inline void CastToFloat::operation(char*& input, float_t& result) {
    doubleCast<float_t>(
        input, strlen(input), result, common::LogicalType{common::LogicalTypeID::FLOAT});
}

template<>
inline void CastToFloat::operation(common::ku_string_t& input, float_t& result) {
    doubleCast<float_t>((char*)input.getData(), input.len, result,
        common::LogicalType{common::LogicalTypeID::FLOAT});
}

struct CastToInt64 {
    template<typename T>
    static inline void operation(T& input, int64_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::RuntimeException{common::stringFormat(
                "Value {} is not within INT64 range", common::TypeUtils::toString(input).c_str())};
        }
    }
};

template<>
inline void CastToInt64::operation(char*& input, int64_t& result) {
    simpleIntegerCast<int64_t, true>(
        input, strlen(input), result, common::LogicalType{common::LogicalTypeID::INT64});
}

template<>
inline void CastToInt64::operation(common::ku_string_t& input, int64_t& result) {
    simpleIntegerCast<int64_t, true>((char*)input.getData(), input.len, result,
        common::LogicalType{common::LogicalTypeID::INT64});
}

struct CastToSerial {
    template<typename T>
    static inline void operation(T& input, int64_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::RuntimeException{common::stringFormat(
                "Value {} is not within INT64 range", common::TypeUtils::toString(input).c_str())};
        }
    }
};

template<>
inline void CastToSerial::operation(common::ku_string_t& input, int64_t& result) {
    simpleIntegerCast<int64_t>((char*)input.getData(), input.len, result,
        common::LogicalType{common::LogicalTypeID::INT64});
}

struct CastToInt32 {
    template<typename T>
    static inline void operation(T& input, int32_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::RuntimeException{common::stringFormat(
                "Value {} is not within INT32 range", common::TypeUtils::toString(input).c_str())};
        }
    }
};

template<>
inline void CastToInt32::operation(char*& input, int32_t& result) {
    simpleIntegerCast<int32_t, true>(
        input, strlen(input), result, common::LogicalType{common::LogicalTypeID::INT32});
}

template<>
inline void CastToInt32::operation(common::ku_string_t& input, int32_t& result) {
    simpleIntegerCast<int32_t, true>((char*)input.getData(), input.len, result,
        common::LogicalType{common::LogicalTypeID::INT32});
}

struct CastToInt16 {
    template<typename T>
    static inline void operation(T& input, int16_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::RuntimeException{common::stringFormat(
                "Value {} is not within INT16 range", common::TypeUtils::toString(input).c_str())};
        }
    }
};

template<>
inline void CastToInt16::operation(common::ku_string_t& input, int16_t& result) {
    simpleIntegerCast<int16_t, true>((char*)input.getData(), input.len, result,
        common::LogicalType{common::LogicalTypeID::INT16});
}

template<>
inline void CastToInt16::operation(char*& input, int16_t& result) {
    simpleIntegerCast<int16_t, true>(
        input, strlen(input), result, common::LogicalType{common::LogicalTypeID::INT16});
}

struct CastToInt8 {
    template<typename T>
    static inline void operation(T& input, int8_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::RuntimeException{common::stringFormat(
                "Value {} is not within INT8 range", common::TypeUtils::toString(input).c_str())};
        }
    }
};

template<>
inline void CastToInt8::operation(common::ku_string_t& input, int8_t& result) {
    simpleIntegerCast<int8_t, true>((char*)input.getData(), input.len, result,
        common::LogicalType{common::LogicalTypeID::INT8});
}

template<>
inline void CastToInt8::operation(char*& input, int8_t& result) {
    simpleIntegerCast<int8_t, true>(
        input, strlen(input), result, common::LogicalType{common::LogicalTypeID::INT8});
}

struct CastToUInt64 {
    template<typename T>
    static inline void operation(T& input, uint64_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::RuntimeException{common::stringFormat(
                "Value {} is not within UINT64 range", common::TypeUtils::toString(input).c_str())};
        }
    }
};

template<>
inline void CastToUInt64::operation(common::ku_string_t& input, uint64_t& result) {
    simpleIntegerCast<uint64_t, false>((char*)input.getData(), input.len, result,
        common::LogicalType{common::LogicalTypeID::UINT64});
}

template<>
inline void CastToUInt64::operation(char*& input, uint64_t& result) {
    simpleIntegerCast<uint64_t, false>(
        input, strlen(input), result, common::LogicalType{common::LogicalTypeID::UINT64});
}

struct CastToUInt32 {
    template<typename T>
    static inline void operation(T& input, uint32_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::RuntimeException{common::stringFormat(
                "Value {} is not within UINT32 range", common::TypeUtils::toString(input).c_str())};
        }
    }
};

template<>
inline void CastToUInt32::operation(common::ku_string_t& input, uint32_t& result) {
    simpleIntegerCast<uint32_t, false>((char*)input.getData(), input.len, result,
        common::LogicalType{common::LogicalTypeID::UINT32});
}

template<>
inline void CastToUInt32::operation(char*& input, uint32_t& result) {
    simpleIntegerCast<uint32_t, false>(
        input, strlen(input), result, common::LogicalType{common::LogicalTypeID::UINT32});
}

struct CastToUInt16 {
    template<typename T>
    static inline void operation(T& input, uint16_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::RuntimeException{common::stringFormat(
                "Value {} is not within UINT16 range", common::TypeUtils::toString(input).c_str())};
        }
    }
};

template<>
inline void CastToUInt16::operation(common::ku_string_t& input, uint16_t& result) {
    simpleIntegerCast<uint16_t, false>((char*)input.getData(), input.len, result,
        common::LogicalType{common::LogicalTypeID::UINT16});
}

template<>
inline void CastToUInt16::operation(char*& input, uint16_t& result) {
    simpleIntegerCast<uint16_t, false>(
        input, strlen(input), result, common::LogicalType{common::LogicalTypeID::UINT16});
}

struct CastToUInt8 {
    template<typename T>
    static inline void operation(T& input, uint8_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::RuntimeException{common::stringFormat(
                "Value {} is not within UINT8 range", common::TypeUtils::toString(input).c_str())};
        }
    }
};

template<>
inline void CastToUInt8::operation(common::ku_string_t& input, uint8_t& result) {
    simpleIntegerCast<uint8_t, false>((char*)input.getData(), input.len, result,
        common::LogicalType{common::LogicalTypeID::UINT8});
}

template<>
inline void CastToUInt8::operation(char*& input, uint8_t& result) {
    simpleIntegerCast<uint8_t, false>(
        input, strlen(input), result, common::LogicalType{common::LogicalTypeID::UINT8});
}

} // namespace function
} // namespace kuzu
