#pragma once

#include <cassert>

#include "src/common/include/type_utils.h"
#include "src/common/include/vector/value_vector.h"
#include "src/common/types/include/value.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct CastToUnstructured {

    template<class INPUT_TYPE>
    static inline void operation(
        const INPUT_TYPE& input, bool isNull, Value& result, const ValueVector& resultValueVector) {
        assert(false);
    }
};

template<>
inline void CastToUnstructured::operation(
    const uint8_t& input, bool isNull, Value& result, const ValueVector& resultValueVector) {
    assert(!isNull);
    result.val.booleanVal = input;
    result.dataType.typeID = BOOL;
}

template<>
inline void CastToUnstructured::operation(
    const int64_t& input, bool isNull, Value& result, const ValueVector& resultValueVector) {
    assert(!isNull);
    result.val.int64Val = input;
    result.dataType.typeID = INT64;
}

template<>
inline void CastToUnstructured::operation(
    const double_t& input, bool isNull, Value& result, const ValueVector& resultValueVector) {
    assert(!isNull);
    result.val.doubleVal = input;
    result.dataType.typeID = DOUBLE;
}

template<>
inline void CastToUnstructured::operation(
    const date_t& input, bool isNull, Value& result, const ValueVector& resultValueVector) {
    assert(!isNull);
    result.val.dateVal = input;
    result.dataType.typeID = DATE;
}

template<>
inline void CastToUnstructured::operation(
    const timestamp_t& input, bool isNull, Value& result, const ValueVector& resultValueVector) {
    assert(!isNull);
    result.val.timestampVal = input;
    result.dataType.typeID = TIMESTAMP;
}

template<>
inline void CastToUnstructured::operation(
    const interval_t& input, bool isNull, Value& result, const ValueVector& resultValueVector) {
    assert(!isNull);
    result.val.intervalVal = input;
    result.dataType.typeID = INTERVAL;
}

template<>
inline void CastToUnstructured::operation(
    const gf_string_t& input, bool isNull, Value& result, const ValueVector& resultValueVector) {
    assert(!isNull);
    TypeUtils::copyString(input, result.val.strVal, resultValueVector.getOverflowBuffer());
    result.dataType.typeID = STRING;
}

struct CastUnstructuredToBool {

    static inline void operation(const Value& input, bool isNull, uint8_t& result) {
        assert(!isNull);
        result = input.val.booleanVal;
    }
};

struct CastUnstructuredToInt64 {

    static inline void operation(const Value& input, bool isNull, int64_t& result) {
        assert(!isNull);
        result = input.val.int64Val;
    }
};

struct CastUnstructuredToTimestamp {

    static inline void operation(const Value& input, bool isNull, timestamp_t& result) {
        assert(!isNull);
        result = input.val.timestampVal;
    }
};

struct CastStringToDate {

    static inline void operation(const gf_string_t& input, bool isNull, date_t& result) {
        assert(!isNull);
        result = Date::FromCString((const char*)input.getData(), input.len);
    }
};

struct CastStringToTimestamp {

    static inline void operation(const gf_string_t& input, bool isNull, timestamp_t& result) {
        assert(!isNull);
        result = Timestamp::FromCString((const char*)input.getData(), input.len);
    }
};

struct CastStringToInterval {

    static inline void operation(const gf_string_t& input, bool isNull, interval_t& result) {
        assert(!isNull);
        result = Interval::FromCString((const char*)input.getData(), input.len);
    }
};

struct CastToString {

    template<typename T>
    static inline string castToStringWithDataType(const T& input, const DataType& dataType) {
        return TypeUtils::toString(input);
    }

    template<typename T>
    static inline void operation(const T& input, bool isNull, gf_string_t& result,
        ValueVector& resultValueVector, const DataType& dataType) {
        assert(!isNull);
        string resultStr = castToStringWithDataType(input, dataType);
        if (resultStr.length() > gf_string_t::SHORT_STR_LENGTH) {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                resultValueVector.getOverflowBuffer().allocateSpace(resultStr.length()));
        }
        result.set(resultStr);
    }
};

template<>
inline string CastToString::castToStringWithDataType(
    const gf_list_t& input, const DataType& dataType) {
    return TypeUtils::toString(input, dataType);
}

} // namespace operation
} // namespace function
} // namespace graphflow
