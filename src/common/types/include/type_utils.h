#pragma once

#include "types_include.h"

namespace graphflow {
namespace common {

class TypeUtils {

public:
    static int64_t convertToInt64(const char* data);

    static double_t convertToDouble(const char* data);

    static bool convertToBoolean(const char* data);

    static size_t getDataTypeSize(DataType dataType);

    static DataType getDataType(const std::string& dataTypeString);

    static string dataTypeToString(DataType dataType);

    static bool isNumericalType(DataType dataType);

    static string toString(bool boolVal) { return boolVal ? "True" : "False"; }

    static string toString(int64_t val) { return to_string(val); }

    static string toString(double val) { return to_string(val); }

    static string toString(nodeID_t val) {
        return to_string(val.label) + ":" + to_string(val.offset);
    }

    static string toString(date_t val) { return Date::toString(val); }

    static string toString(timestamp_t val) { return Timestamp::toString(val); }

    static string toString(interval_t val) { return Interval::toString(val); }

private:
    static void throwConversionExceptionOutOfRange(const char* data, DataType dataType);
    static void throwConversionExceptionIfNoOrNotEveryCharacterIsConsumed(
        const char* data, const char* eptr, DataType dataType);
    static string prefixConversionExceptionMessage(const char* data, DataType dataType);
};

} // namespace common
} // namespace graphflow
