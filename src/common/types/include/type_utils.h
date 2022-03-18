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

    static inline string toString(bool boolVal) { return boolVal ? "True" : "False"; }

    static inline string toString(int64_t val) { return to_string(val); }

    static inline string toString(double val) { return to_string(val); }

    static inline string toString(nodeID_t val) {
        return to_string(val.label) + ":" + to_string(val.offset);
    }

    static inline string toString(date_t val) { return Date::toString(val); }

    static inline string toString(timestamp_t val) { return Timestamp::toString(val); }

    static inline string toString(interval_t val) { return Interval::toString(val); }

    static inline string toString(gf_string_t val) { return val.getAsString(); }

    static inline void encodeOverflowPtr(
        uint64_t& overflowPtr, uint64_t pageIdx, uint16_t pageOffset) {
        memcpy(&overflowPtr, &pageIdx, 6);
        memcpy(((uint8_t*)&overflowPtr) + 6, &pageOffset, 2);
    }

    static inline void decodeOverflowPtr(
        uint64_t overflowPtr, uint64_t& pageIdx, uint16_t& pageOffset) {
        pageIdx = 0;
        memcpy(&pageIdx, &overflowPtr, 6);
        memcpy(&pageOffset, ((uint8_t*)&overflowPtr) + 6, 2);
    }

private:
    static void throwConversionExceptionOutOfRange(const char* data, DataType dataType);
    static void throwConversionExceptionIfNoOrNotEveryCharacterIsConsumed(
        const char* data, const char* eptr, DataType dataType);
    static string prefixConversionExceptionMessage(const char* data, DataType dataType);
};

} // namespace common
} // namespace graphflow
