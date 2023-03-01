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
    static inline std::string toString(double val) { return std::to_string(val); }
    static inline std::string toString(const nodeID_t& val) {
        return std::to_string(val.tableID) + ":" + std::to_string(val.offset);
    }
    static inline std::string toString(const date_t& val) { return Date::toString(val); }
    static inline std::string toString(const timestamp_t& val) { return Timestamp::toString(val); }
    static inline std::string toString(const interval_t& val) { return Interval::toString(val); }
    static inline std::string toString(const ku_string_t& val) { return val.getAsString(); }
    static inline std::string toString(const std::string& val) { return val; }
    static std::string toString(const ku_list_t& val, const DataType& dataType);

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
    static inline bool isValueEqual(
        T& left, T& right, const DataType& leftDataType, const DataType& rightDataType) {
        return left == right;
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
    static std::string elementToString(
        const DataType& dataType, uint8_t* overflowPtr, uint64_t pos);

    static std::string prefixConversionExceptionMessage(const char* data, DataTypeID dataTypeID);
};

template<>
inline bool TypeUtils::isValueEqual(ku_list_t& left, ku_list_t& right, const DataType& leftDataType,
    const DataType& rightDataType) {
    if (leftDataType != rightDataType || left.size != right.size) {
        return false;
    }

    for (auto i = 0u; i < left.size; i++) {
        switch (leftDataType.childType->typeID) {
        case BOOL: {
            if (!isValueEqual(reinterpret_cast<uint8_t*>(left.overflowPtr)[i],
                    reinterpret_cast<uint8_t*>(right.overflowPtr)[i], *leftDataType.childType,
                    *rightDataType.childType)) {
                return false;
            }
        } break;
        case INT64: {
            if (!isValueEqual(reinterpret_cast<int64_t*>(left.overflowPtr)[i],
                    reinterpret_cast<int64_t*>(right.overflowPtr)[i], *leftDataType.childType,
                    *rightDataType.childType)) {
                return false;
            }
        } break;
        case DOUBLE: {
            if (!isValueEqual(reinterpret_cast<double_t*>(left.overflowPtr)[i],
                    reinterpret_cast<double_t*>(right.overflowPtr)[i], *leftDataType.childType,
                    *rightDataType.childType)) {
                return false;
            }
        } break;
        case STRING: {
            if (!isValueEqual(reinterpret_cast<ku_string_t*>(left.overflowPtr)[i],
                    reinterpret_cast<ku_string_t*>(right.overflowPtr)[i], *leftDataType.childType,
                    *rightDataType.childType)) {
                return false;
            }
        } break;
        case DATE: {
            if (!isValueEqual(reinterpret_cast<date_t*>(left.overflowPtr)[i],
                    reinterpret_cast<date_t*>(right.overflowPtr)[i], *leftDataType.childType,
                    *rightDataType.childType)) {
                return false;
            }
        } break;
        case TIMESTAMP: {
            if (!isValueEqual(reinterpret_cast<timestamp_t*>(left.overflowPtr)[i],
                    reinterpret_cast<timestamp_t*>(right.overflowPtr)[i], *leftDataType.childType,
                    *rightDataType.childType)) {
                return false;
            }
        } break;
        case INTERVAL: {
            if (!isValueEqual(reinterpret_cast<interval_t*>(left.overflowPtr)[i],
                    reinterpret_cast<interval_t*>(right.overflowPtr)[i], *leftDataType.childType,
                    *rightDataType.childType)) {
                return false;
            }
        } break;
        case VAR_LIST: {
            if (!isValueEqual(reinterpret_cast<ku_list_t*>(left.overflowPtr)[i],
                    reinterpret_cast<ku_list_t*>(right.overflowPtr)[i], *leftDataType.childType,
                    *rightDataType.childType)) {
                return false;
            }
        } break;
        default: {
            throw RuntimeException("Unsupported data type " +
                                   Types::dataTypeToString(leftDataType) +
                                   " for TypeUtils::isValueEqual.");
        }
        }
    }
    return true;
}

} // namespace common
} // namespace kuzu
