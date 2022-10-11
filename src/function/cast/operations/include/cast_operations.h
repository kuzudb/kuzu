#pragma once

#include <cassert>

#include "src/common/include/in_mem_overflow_buffer_utils.h"
#include "src/common/include/type_utils.h"
#include "src/common/include/vector/value_vector.h"
#include "src/common/types/include/value.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct CastToUnstructured {
    template<class INPUT_TYPE>
    static inline void operation(INPUT_TYPE& input, Value& result, ValueVector& resultVector) {
        assert(false);
    }
};

template<>
inline void CastToUnstructured::operation(
    uint8_t& input, Value& result, ValueVector& resultVector) {
    result.val.booleanVal = input;
    result.dataType.typeID = BOOL;
}

template<>
inline void CastToUnstructured::operation(
    int64_t& input, Value& result, ValueVector& resultVector) {
    result.val.int64Val = input;
    result.dataType.typeID = INT64;
}

template<>
inline void CastToUnstructured::operation(
    double_t& input, Value& result, ValueVector& resultVector) {
    result.val.doubleVal = input;
    result.dataType.typeID = DOUBLE;
}

template<>
inline void CastToUnstructured::operation(date_t& input, Value& result, ValueVector& resultVector) {
    result.val.dateVal = input;
    result.dataType.typeID = DATE;
}

template<>
inline void CastToUnstructured::operation(
    timestamp_t& input, Value& result, ValueVector& resultVector) {
    result.val.timestampVal = input;
    result.dataType.typeID = TIMESTAMP;
}

template<>
inline void CastToUnstructured::operation(
    interval_t& input, Value& result, ValueVector& resultVector) {
    result.val.intervalVal = input;
    result.dataType.typeID = INTERVAL;
}

template<>
inline void CastToUnstructured::operation(
    gf_string_t& input, Value& result, ValueVector& resultVector) {
    InMemOverflowBufferUtils::copyString(
        input, result.val.strVal, resultVector.getOverflowBuffer());
    result.dataType.typeID = STRING;
}

struct CastUnstructuredToBool {
    static inline void operation(Value& input, uint8_t& result) { result = input.val.booleanVal; }
};

struct CastUnstructuredToInt64 {
    static inline void operation(Value& input, int64_t& result) { result = input.val.int64Val; }
};

struct CastUnstructuredToTimestamp {
    static inline void operation(Value& input, timestamp_t& result) {
        result = input.val.timestampVal;
    }
};

struct CastStringToDate {
    static inline void operation(gf_string_t& input, date_t& result) {
        result = Date::FromCString((const char*)input.getData(), input.len);
    }
};

struct CastStringToTimestamp {
    static inline void operation(gf_string_t& input, timestamp_t& result) {
        result = Timestamp::FromCString((const char*)input.getData(), input.len);
    }
};

struct CastStringToInterval {
    static inline void operation(gf_string_t& input, interval_t& result) {
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
        T& input, gf_string_t& result, ValueVector& resultVector, const DataType& dataType) {
        string resultStr = castToStringWithDataType(input, dataType);
        if (resultStr.length() > gf_string_t::SHORT_STR_LENGTH) {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                resultVector.getOverflowBuffer().allocateSpace(resultStr.length()));
        }
        result.set(resultStr);
    }
};

template<>
inline string CastToString::castToStringWithDataType(gf_list_t& input, const DataType& dataType) {
    return TypeUtils::toString(input, dataType);
}

} // namespace operation
} // namespace function
} // namespace graphflow
