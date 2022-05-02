#pragma once

#include "src/common/include/overflow_buffer.h"
#include "src/common/types/include/literal.h"
#include "src/common/types/include/types_include.h"
#include "src/common/types/include/value.h"

namespace graphflow {
namespace common {

class TypeUtils {

public:
    static int64_t convertToInt64(const char* data);
    static double_t convertToDouble(const char* data);
    static uint32_t convertToUint32(const char* data);
    static bool convertToBoolean(const char* data);

    static inline string toString(bool boolVal) { return boolVal ? "True" : "False"; }
    static inline string toString(int64_t val) { return to_string(val); }
    static inline string toString(double val) { return to_string(val); }
    static inline string toString(const nodeID_t& val) {
        return to_string(val.label) + ":" + to_string(val.offset);
    }
    static inline string toString(const date_t& val) { return Date::toString(val); }
    static inline string toString(const timestamp_t& val) { return Timestamp::toString(val); }
    static inline string toString(const interval_t& val) { return Interval::toString(val); }
    static inline string toString(const gf_string_t& val) { return val.getAsString(); }
    static inline string toString(const string& val) { return val; }
    static string toString(const gf_list_t& val, const DataType& dataType);
    static string toString(const Literal& val);
    static string toString(const Value& val);

    static void castLiteralToString(Literal& literal);

    // Currently, this function is only used in `allocateStringIfNecessary`. Make this function
    // private once we remove allocateStringIfNecessary.
    static inline void allocateSpaceForStringIfNecessary(
        gf_string_t& result, uint64_t numBytes, OverflowBuffer& buffer) {
        if (gf_string_t::isShortString(numBytes)) {
            return;
        }
        result.overflowPtr = reinterpret_cast<uint64_t>(buffer.allocateSpace(numBytes));
    }

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

    static void copyString(
        const char* src, uint64_t len, gf_string_t& dest, OverflowBuffer& overflowBuffer);
    static void copyString(const string& src, gf_string_t& dest, OverflowBuffer& overflowBuffer);
    static void copyString(
        const gf_string_t& src, gf_string_t& dest, OverflowBuffer& overflowBuffer);

    static void copyListNonRecursive(const uint8_t* srcValues, gf_list_t& dest,
        const DataType& dataType, OverflowBuffer& overflowBuffer);
    static void copyListNonRecursive(const vector<uint8_t*>& srcValues, gf_list_t& dest,
        const DataType& dataType, OverflowBuffer& overflowBuffer);
    static void copyListRecursiveIfNested(const gf_list_t& src, gf_list_t& dest,
        const DataType& dataType, OverflowBuffer& overflowBuffer, uint32_t srcStartIdx = 0,
        uint32_t srcEndIdx = UINT32_MAX);

    template<typename T>
    static inline void setListElement(gf_list_t& result, uint64_t elementPos, T& element,
        const DataType& dataType, OverflowBuffer& overflowBuffer) {
        reinterpret_cast<T*>(result.overflowPtr)[elementPos] = element;
    }

    template<typename T>
    static inline bool isValueEqual(
        T& left, T& right, const DataType& leftDataType, const DataType& rightDataType) {
        return left == right;
    }

private:
    static string elementToString(const DataType& dataType, uint8_t* overflowPtr, uint64_t pos);
    static inline void allocateSpaceForList(
        gf_list_t& list, uint64_t numBytes, OverflowBuffer& buffer) {
        list.overflowPtr = reinterpret_cast<uint64_t>(buffer.allocateSpace(numBytes));
    }

    static void throwConversionExceptionOutOfRange(const char* data, DataTypeID dataTypeID);
    static void throwConversionExceptionIfNoOrNotEveryCharacterIsConsumed(
        const char* data, const char* eptr, DataTypeID dataTypeID);
    static string prefixConversionExceptionMessage(const char* data, DataTypeID dataTypeID);
};

template<>
inline void TypeUtils::setListElement(gf_list_t& result, uint64_t elementPos, gf_string_t& element,
    const DataType& dataType, OverflowBuffer& overflowBuffer) {
    gf_string_t elementToAppend;
    TypeUtils::copyString(element, elementToAppend, overflowBuffer);
    reinterpret_cast<gf_string_t*>(result.overflowPtr)[elementPos] = elementToAppend;
}

template<>
inline void TypeUtils::setListElement(gf_list_t& result, uint64_t elementPos, gf_list_t& element,
    const DataType& dataType, OverflowBuffer& overflowBuffer) {
    gf_list_t elementToAppend;
    TypeUtils::copyListRecursiveIfNested(
        element, elementToAppend, *dataType.childType, overflowBuffer);
    reinterpret_cast<gf_list_t*>(result.overflowPtr)[elementPos] = elementToAppend;
}

template<>
inline bool TypeUtils::isValueEqual(gf_list_t& left, gf_list_t& right, const DataType& leftDataType,
    const DataType& rightDataType) {
    if (leftDataType != rightDataType) {
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
            if (!isValueEqual(reinterpret_cast<gf_string_t*>(left.overflowPtr)[i],
                    reinterpret_cast<gf_string_t*>(right.overflowPtr)[i], *leftDataType.childType,
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
        case LIST: {
            if (!isValueEqual(reinterpret_cast<gf_list_t*>(left.overflowPtr)[i],
                    reinterpret_cast<gf_list_t*>(right.overflowPtr)[i], *leftDataType.childType,
                    *rightDataType.childType)) {
                return false;
            }
        } break;
        default: {
            assert(false);
        }
        }
    }
    return true;
}

} // namespace common
} // namespace graphflow
