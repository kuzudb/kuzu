#pragma once

#include <sstream>

#include "common/exception.h"
#include "common/types/types_include.h"

namespace kuzu {
namespace common {

class TypeUtils {

public:
    static uint32_t convertToUint32(const char* data);
    static bool convertToBoolean(const char* data);

    static inline std::string toString(bool boolVal) { return boolVal ? "True" : "False"; }
    static inline std::string toString(int64_t val) { return std::to_string(val); }
    static inline std::string toString(int32_t val) { return std::to_string(val); }
    static inline std::string toString(int16_t val) { return std::to_string(val); }
    static inline std::string toString(double_t val) { return std::to_string(val); }
    static inline std::string toString(float_t val) { return std::to_string(val); }
    static inline std::string toString(const internalID_t& val) {
        return std::to_string(val.tableID) + ":" + std::to_string(val.offset);
    }
    static inline std::string toString(const date_t& val) { return Date::toString(val); }
    static inline std::string toString(const timestamp_t& val) { return Timestamp::toString(val); }
    static inline std::string toString(const interval_t& val) { return Interval::toString(val); }
    static inline std::string toString(const ku_string_t& val) { return val.getAsString(); }
    static inline std::string toString(const std::string& val) { return val; }
    static std::string toString(const list_entry_t& val, void* valueVector);
    static std::string toString(const struct_entry_t& val, void* valueVector);

    static inline void encodeOverflowPtr(
        uint64_t& overflowPtr, page_idx_t pageIdx, uint16_t pageOffset) {
        memcpy(&overflowPtr, &pageIdx, 4);
        memcpy(((uint8_t*)&overflowPtr) + 4, &pageOffset, 2);
    }
    static inline void decodeOverflowPtr(
        uint64_t overflowPtr, page_idx_t& pageIdx, uint16_t& pageOffset) {
        pageIdx = 0;
        memcpy(&pageIdx, &overflowPtr, 4);
        memcpy(&pageOffset, ((uint8_t*)&overflowPtr) + 4, 2);
    }

    template<typename T>
    static T convertStringToNumber(const char* data) {
        std::istringstream iss{data};
        if (iss.str().empty()) {
            throw ConversionException{"Empty string."};
        }
        T retVal;
        iss >> retVal;
        if (iss.fail() || !iss.eof()) {
            throw ConversionException{"Invalid number: " + std::string{data} + "."};
        }
        return retVal;
    }

private:
    static std::string castValueToString(const LogicalType& dataType, uint8_t* value, void* vector);

    static std::string prefixConversionExceptionMessage(const char* data, LogicalTypeID dataTypeID);
};

} // namespace common
} // namespace kuzu
