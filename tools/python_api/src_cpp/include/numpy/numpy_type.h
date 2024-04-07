#pragma once

#include "common/types/types.h"
#include "pybind_include.h"

namespace kuzu {

// Pandas has two different sets of types
// NumPy dtypes (e.g., bool, int8,...)
// Pandas Specific Types (e.g., categorical, datetime_tz,...)
// TODO(Ziyi): Support more timestamp types, object and category(enum) type.
enum class NumpyNullableType : uint8_t {
    //! NumPy dtypes
    BOOL,        //! bool_, bool8
    INT_8,       //! byte, int8
    UINT_8,      //! ubyte, uint8
    INT_16,      //! int16, short
    UINT_16,     //! uint16, ushort
    INT_32,      //! int32, intc
    UINT_32,     //! uint32, uintc,
    INT_64,      //! int64, int0, int_, intp, matrix
    UINT_64,     //! uint64, uint, uint0, uintp
    FLOAT_16,    //! float16, half
    FLOAT_32,    //! float32, single
    FLOAT_64,    //! float64, float_, double
    OBJECT,      //! object
    DATETIME_S,  //! datetime64[s], <M8[s]
    DATETIME_MS, //! datetime64[ms], <M8[ms]
    DATETIME_NS, //! datetime64[ns], <M8[ns]
    DATETIME_US, //! datetime64[us], <M8[us]
    TIMEDELTA,   //! timedelta64[D], timedelta64
};

struct NumpyType {
    NumpyNullableType type;
    bool hasTimezone = false;
};

struct NumpyTypeUtils {
    static NumpyType convertNumpyType(const py::handle& colType);
    static std::unique_ptr<common::LogicalType> numpyToLogicalType(const NumpyType& npType);
};

} // namespace kuzu
