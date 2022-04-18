#pragma once

#include <cassert>

#include "src/common/types/include/types_include.h"
#include "src/common/types/include/value.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct CastToUnstructured {

    template<class INPUT_TYPE>
    static inline void operation(const INPUT_TYPE& input, bool isNull, Value& result) {
        assert(false);
    }
};

template<>
inline void CastToUnstructured::operation(const uint8_t& input, bool isNull, Value& result) {
    assert(!isNull);
    result.val.booleanVal = input;
    result.dataType.typeID = BOOL;
}

template<>
inline void CastToUnstructured::operation(const int64_t& input, bool isNull, Value& result) {
    assert(!isNull);
    result.val.int64Val = input;
    result.dataType.typeID = INT64;
}

template<>
inline void CastToUnstructured::operation(const double_t& input, bool isNull, Value& result) {
    assert(!isNull);
    result.val.doubleVal = input;
    result.dataType.typeID = DOUBLE;
}

template<>
inline void CastToUnstructured::operation(const date_t& input, bool isNull, Value& result) {
    assert(!isNull);
    result.val.dateVal = input;
    result.dataType.typeID = DATE;
}

template<>
inline void CastToUnstructured::operation(const timestamp_t& input, bool isNull, Value& result) {
    assert(!isNull);
    result.val.timestampVal = input;
    result.dataType.typeID = TIMESTAMP;
}

template<>
inline void CastToUnstructured::operation(const interval_t& input, bool isNull, Value& result) {
    assert(!isNull);
    result.val.intervalVal = input;
    result.dataType.typeID = INTERVAL;
}

struct CastUnstructuredToBool {

    static inline void operation(const Value& input, bool isNull, uint8_t& result) {
        assert(!isNull);
        result = input.val.booleanVal;
    }
};

struct CastUnstructuredToDate {

    static inline void operation(const Value& input, bool isNull, date_t& result) {
        assert(!isNull);
        result = input.val.dateVal;
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

} // namespace operation
} // namespace function
} // namespace graphflow
