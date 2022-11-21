#pragma once

#include <cassert>

#include "common/in_mem_overflow_buffer_utils.h"
#include "common/type_utils.h"
#include "common/vector/value_vector.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

struct CastStringToDate {
    static inline void operation(ku_string_t& input, date_t& result) {
        result = Date::FromCString((const char*)input.getData(), input.len);
    }
};

struct CastStringToTimestamp {
    static inline void operation(ku_string_t& input, timestamp_t& result) {
        result = Timestamp::FromCString((const char*)input.getData(), input.len);
    }
};

struct CastStringToInterval {
    static inline void operation(ku_string_t& input, interval_t& result) {
        result = Interval::FromCString((const char*)input.getData(), input.len);
    }
};

struct CastToString {

    template<typename T>
    static inline string castToStringWithDataType(T& input, const DataType& dataType) {
        return TypeUtils::toString(input);
    }

    template<typename T>
    static inline void operation(
        T& input, ku_string_t& result, ValueVector& resultVector, const DataType& dataType) {
        string resultStr = castToStringWithDataType(input, dataType);
        if (resultStr.length() > ku_string_t::SHORT_STR_LENGTH) {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                resultVector.getOverflowBuffer().allocateSpace(resultStr.length()));
        }
        result.set(resultStr);
    }
};

template<>
inline string CastToString::castToStringWithDataType(ku_list_t& input, const DataType& dataType) {
    return TypeUtils::toString(input, dataType);
}

} // namespace operation
} // namespace function
} // namespace kuzu
