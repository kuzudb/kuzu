#pragma once

#include <sstream>

#include "common/exception.h"
#include "common/types/types_include.h"

namespace kuzu {
namespace common {

// This class is used to cast c++ const char* to c++ primitive types. Should be moved to parser
// once the csv parser is implemented.
class StringCastUtils {

public:
    static bool tryCastToBoolean(const char* data, uint64_t length, bool& result);
    static bool castToBool(const char* data, uint64_t length);
    template<typename T>
    static bool tryCastToNum(const char* data, uint64_t length, T& result) {
        auto numStr = std::string{data, length};
        removeSpace(numStr);
        std::istringstream iss{numStr};
        if (iss.str().empty()) {
            throw ConversionException{"Empty string."};
        }
        iss >> result;
        if (iss.fail() || !iss.eof()) {
            return false;
        }
        return true;
    }
    template<typename T>
    static T castToNum(const char* data, uint64_t length) {
        T result;
        if (!tryCastToNum(data, length, result)) {
            throw ConversionException{"Invalid number: " + std::string{data} + "."};
        }
        return result;
    }

private:
    static void removeSpace(std::string& str);
};

class TypeUtils {

public:
    static uint32_t convertToUint32(const char* data);

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

    static std::string prefixConversionExceptionMessage(const char* data, LogicalTypeID dataTypeID);

private:
    static std::string castValueToString(const LogicalType& dataType, uint8_t* value, void* vector);
};

} // namespace common
} // namespace kuzu
