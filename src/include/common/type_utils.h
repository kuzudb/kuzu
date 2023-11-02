#pragma once

#include <cmath>

#include "common/types/date_t.h"
#include "common/types/int128_t.h"
#include "common/types/interval_t.h"
#include "common/types/ku_string.h"
#include "common/types/timestamp_t.h"
#include "common/types/types.h"

namespace kuzu {
namespace common {

struct blob_t;

class TypeUtils {

public:
    template<typename T>
    static inline std::string toString(const T& val, void* /*valueVector*/ = nullptr) {
        static_assert(std::is_same<T, int64_t>::value || std::is_same<T, int32_t>::value ||
                      std::is_same<T, int16_t>::value || std::is_same<T, int8_t>::value ||
                      std::is_same<T, uint64_t>::value || std::is_same<T, uint32_t>::value ||
                      std::is_same<T, uint16_t>::value || std::is_same<T, uint8_t>::value ||
                      std::is_same<T, double_t>::value || std::is_same<T, float_t>::value);
        return std::to_string(val);
    }

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

    static std::string castValueToString(
        const LogicalType& dataType, const uint8_t* value, void* vector);
};

// Forward declaration of template specializations.
template<>
std::string TypeUtils::toString(const int128_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const bool& val, void* valueVector);
template<>
std::string TypeUtils::toString(const internalID_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const date_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const timestamp_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const interval_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const ku_string_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const blob_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const list_entry_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const map_entry_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const struct_entry_t& val, void* valueVector);
template<>
std::string TypeUtils::toString(const union_entry_t& val, void* valueVector);

} // namespace common
} // namespace kuzu
