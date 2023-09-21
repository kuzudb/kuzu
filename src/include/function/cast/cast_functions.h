#pragma once

#include <cassert>

#include "common/exception/runtime.h"
#include "common/type_utils.h"
#include "common/types/blob.h"
#include "common/types/ku_string.h"
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

template<typename SRC, typename DST>
static inline void numericDownCast(SRC& input, DST& result, const std::string& dstTypeStr) {
    if (input < std::numeric_limits<DST>::min() || input > std::numeric_limits<DST>::max()) {
        throw common::RuntimeException(
            "Cast failed. " + std::to_string(input) + " is not in " + dstTypeStr + " range.");
    }
    result = (DST)input;
}

struct CastToDouble {
    template<typename T>
    static inline void operation(T& input, double_t& result) {
        result = static_cast<double_t>(input);
    }
};

struct CastToFloat {
    template<typename T>
    static inline void operation(T& input, float_t& result) {
        result = static_cast<float_t>(input);
    }
};

template<>
inline void CastToFloat::operation(double_t& input, float_t& result) {
    numericDownCast<double_t, float_t>(input, result, "FLOAT");
}

struct CastToInt64 {
    template<typename T>
    static inline void operation(T& input, int64_t& result) {
        result = static_cast<int64_t>(input);
    }
};

template<>
inline void CastToInt64::operation(double_t& input, int64_t& result) {
    numericDownCast<double_t, int64_t>(input, result, "INT64");
}

template<>
inline void CastToInt64::operation(float_t& input, int64_t& result) {
    numericDownCast<float_t, int64_t>(input, result, "INT64");
}

struct CastToSerial {
    template<typename T>
    static inline void operation(T& input, int64_t& result) {
        CastToInt64::operation(input, result);
    }
};

struct CastToInt32 {
    template<typename T>
    static inline void operation(T& input, int32_t& result) {
        result = static_cast<int32_t>(input);
    }
};

template<>
inline void CastToInt32::operation(double_t& input, int32_t& result) {
    numericDownCast<double_t, int32_t>(input, result, "INT32");
}

template<>
inline void CastToInt32::operation(float_t& input, int32_t& result) {
    numericDownCast<float_t, int32_t>(input, result, "INT32");
}

template<>
inline void CastToInt32::operation(int64_t& input, int32_t& result) {
    numericDownCast<int64_t, int32_t>(input, result, "INT32");
}

struct CastToInt16 {
    template<typename T>
    static inline void operation(T& input, int16_t& result) {
        result = static_cast<int16_t>(input);
    }
};

template<>
inline void CastToInt16::operation(double_t& input, int16_t& result) {
    numericDownCast<double_t, int16_t>(input, result, "INT16");
}

template<>
inline void CastToInt16::operation(float_t& input, int16_t& result) {
    numericDownCast<float_t, int16_t>(input, result, "INT16");
}

template<>
inline void CastToInt16::operation(int64_t& input, int16_t& result) {
    numericDownCast<int64_t, int16_t>(input, result, "INT16");
}

template<>
inline void CastToInt16::operation(int32_t& input, int16_t& result) {
    numericDownCast<int32_t, int16_t>(input, result, "INT16");
}

struct CastToInt8 {
    template<typename T>
    static inline void operation(T& input, int8_t& result) {
        result = static_cast<int8_t>(input);
    }
};

template<>
inline void CastToInt8::operation(double_t& input, int8_t& result) {
    numericDownCast<double_t, int8_t>(input, result, "INT8");
}

template<>
inline void CastToInt8::operation(float_t& input, int8_t& result) {
    numericDownCast<float_t, int8_t>(input, result, "INT8");
}

template<>
inline void CastToInt8::operation(int64_t& input, int8_t& result) {
    numericDownCast<int64_t, int8_t>(input, result, "INT8");
}

template<>
inline void CastToInt8::operation(int32_t& input, int8_t& result) {
    numericDownCast<int32_t, int8_t>(input, result, "INT8");
}

template<>
inline void CastToInt8::operation(int16_t& input, int8_t& result) {
    numericDownCast<int16_t, int8_t>(input, result, "INT8");
}

struct CastToUInt64 {
    template<typename T>
    static inline void operation(T& input, uint64_t& result) {
        result = static_cast<uint64_t>(input);
    }
};

template<>
inline void CastToUInt64::operation(double_t& input, uint64_t& result) {
    numericDownCast<double_t, uint64_t>(input, result, "UINT64");
}

template<>
inline void CastToUInt64::operation(float_t& input, uint64_t& result) {
    numericDownCast<float_t, uint64_t>(input, result, "UINT64");
}

struct CastToUInt32 {
    template<typename T>
    static inline void operation(T& input, uint32_t& result) {
        result = static_cast<uint32_t>(input);
    }
};

template<>
inline void CastToUInt32::operation(double_t& input, uint32_t& result) {
    numericDownCast<double_t, uint32_t>(input, result, "UINT32");
}

template<>
inline void CastToUInt32::operation(float_t& input, uint32_t& result) {
    numericDownCast<float_t, uint32_t>(input, result, "UINT32");
}

template<>
inline void CastToUInt32::operation(int64_t& input, uint32_t& result) {
    numericDownCast<int64_t, uint32_t>(input, result, "UINT32");
}

template<>
inline void CastToUInt32::operation(uint64_t& input, uint32_t& result) {
    numericDownCast<uint64_t, uint32_t>(input, result, "UINT32");
}

struct CastToUInt16 {
    template<typename T>
    static inline void operation(T& input, uint16_t& result) {
        result = static_cast<uint16_t>(input);
    }
};

template<>
inline void CastToUInt16::operation(double_t& input, uint16_t& result) {
    numericDownCast<double_t, uint16_t>(input, result, "UINT16");
}

template<>
inline void CastToUInt16::operation(float_t& input, uint16_t& result) {
    numericDownCast<float_t, uint16_t>(input, result, "UINT16");
}

template<>
inline void CastToUInt16::operation(int64_t& input, uint16_t& result) {
    numericDownCast<int64_t, uint16_t>(input, result, "UINT16");
}

template<>
inline void CastToUInt16::operation(uint64_t& input, uint16_t& result) {
    numericDownCast<uint64_t, uint16_t>(input, result, "UINT16");
}

template<>
inline void CastToUInt16::operation(int32_t& input, uint16_t& result) {
    numericDownCast<int32_t, uint16_t>(input, result, "UINT16");
}

template<>
inline void CastToUInt16::operation(uint32_t& input, uint16_t& result) {
    numericDownCast<uint32_t, uint16_t>(input, result, "UINT16");
}

struct CastToUInt8 {
    template<typename T>
    static inline void operation(T& input, uint8_t& result) {
        result = static_cast<uint8_t>(input);
    }
};

template<>
inline void CastToUInt8::operation(double_t& input, uint8_t& result) {
    numericDownCast<double_t, uint8_t>(input, result, "UINT8");
}

template<>
inline void CastToUInt8::operation(float_t& input, uint8_t& result) {
    numericDownCast<float_t, uint8_t>(input, result, "UINT8");
}

template<>
inline void CastToUInt8::operation(int64_t& input, uint8_t& result) {
    numericDownCast<int64_t, uint8_t>(input, result, "UINT8");
}

template<>
inline void CastToUInt8::operation(uint64_t& input, uint8_t& result) {
    numericDownCast<uint64_t, uint8_t>(input, result, "UINT8");
}

template<>
inline void CastToUInt8::operation(int32_t& input, uint8_t& result) {
    numericDownCast<int32_t, uint8_t>(input, result, "UINT8");
}

template<>
inline void CastToUInt8::operation(uint32_t& input, uint8_t& result) {
    numericDownCast<uint32_t, uint8_t>(input, result, "UINT8");
}

template<>
inline void CastToUInt8::operation(int16_t& input, uint8_t& result) {
    numericDownCast<int16_t, uint8_t>(input, result, "UINT8");
}

template<>
inline void CastToUInt8::operation(uint16_t& input, uint8_t& result) {
    numericDownCast<uint16_t, uint8_t>(input, result, "UINT8");
}

} // namespace function
} // namespace kuzu
