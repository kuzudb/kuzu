#pragma once

#include <sstream>

#include "common/exception/conversion.h"
#include "common/types/date_t.h"
#include "common/types/interval_t.h"
#include "common/types/ku_string.h"
#include "common/types/timestamp_t.h"
#include "common/types/types.h"

namespace kuzu {
namespace common {

class TypeUtils {

public:
    static uint32_t convertToUint32(const char* data);

    static inline std::string toString(bool boolVal) { return boolVal ? "True" : "False"; }
    static inline std::string toString(int64_t val) { return std::to_string(val); }
    static inline std::string toString(int32_t val) { return std::to_string(val); }
    static inline std::string toString(int16_t val) { return std::to_string(val); }
    static inline std::string toString(int8_t val) { return std::to_string(val); }
    static inline std::string toString(uint64_t val) { return std::to_string(val); }
    static inline std::string toString(uint32_t val) { return std::to_string(val); }
    static inline std::string toString(uint16_t val) { return std::to_string(val); }
    static inline std::string toString(uint8_t val) { return std::to_string(val); }
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
