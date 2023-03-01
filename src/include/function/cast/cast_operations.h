#pragma once

#include <cassert>

#include "common/in_mem_overflow_buffer_utils.h"
#include "common/type_utils.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {
namespace operation {

struct CastStringToDate {
    static inline void operation(common::ku_string_t& input, common::date_t& result) {
        result = common::Date::FromCString((const char*)input.getData(), input.len);
    }
};

struct CastStringToTimestamp {
    static inline void operation(common::ku_string_t& input, common::timestamp_t& result) {
        result = common::Timestamp::FromCString((const char*)input.getData(), input.len);
    }
};

struct CastStringToInterval {
    static inline void operation(common::ku_string_t& input, common::interval_t& result) {
        result = common::Interval::FromCString((const char*)input.getData(), input.len);
    }
};

struct CastToString {

    template<typename T>
    static inline std::string castToStringWithDataType(T& input, const common::DataType& dataType) {
        return common::TypeUtils::toString(input);
    }

    template<typename T>
    static inline void operation(T& input, common::ku_string_t& result,
        common::ValueVector& resultVector, const common::DataType& dataType) {
        std::string resultStr = castToStringWithDataType(input, dataType);
        if (resultStr.length() > common::ku_string_t::SHORT_STR_LENGTH) {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                resultVector.getOverflowBuffer().allocateSpace(resultStr.length()));
        }
        result.set(resultStr);
    }
};

template<>
inline std::string CastToString::castToStringWithDataType(
    common::ku_list_t& input, const common::DataType& dataType) {
    return common::TypeUtils::toString(input, dataType);
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

struct CastToInt64 {
    template<typename T>
    static inline void operation(T& input, int64_t& result) {
        result = static_cast<int64_t>(input);
    }
};

struct CastToInt32 {
    template<typename T>
    static inline void operation(T& input, int32_t& result) {
        result = static_cast<int32_t>(input);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
