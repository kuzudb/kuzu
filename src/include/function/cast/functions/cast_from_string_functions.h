#pragma once

#include "cast_string_non_nested_functions.h"
#include "common/copier_config/csv_reader_config.h"
#include "common/type_utils.h"
#include "common/types/blob.h"
#include "common/vector/value_vector.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct CastString {
    static void copyStringToVector(
        ValueVector* vector, uint64_t rowToAdd, std::string_view strVal, const CSVOption* option);

    template<typename T>
    static inline bool tryCast(const ku_string_t& input, T& result) {
        // try cast for signed integer types
        return trySimpleIntegerCast<T, true>(
            reinterpret_cast<const char*>(input.getData()), input.len, result);
    }

    template<typename T>
    static inline void operation(const ku_string_t& input, T& result,
        ValueVector* /*resultVector*/ = nullptr, uint64_t /*rowToAdd*/ = 0,
        const CSVOption* /*option*/ = nullptr) {
        // base case: int64
        simpleIntegerCast<T, true>(reinterpret_cast<const char*>(input.getData()), input.len,
            result, LogicalType{LogicalTypeID::INT64});
    }

    static void castToFixedList(const ku_string_t& input, ValueVector* resultVector,
        uint64_t rowToAdd, const CSVOption* option);
};

template<>
inline void CastString::operation(const ku_string_t& input, int128_t& result,
    ValueVector* /*resultVector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    simpleInt128Cast(reinterpret_cast<const char*>(input.getData()), input.len, result);
}

template<>
inline void CastString::operation(const ku_string_t& input, int32_t& result,
    ValueVector* /*resultVector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    simpleIntegerCast<int32_t>(reinterpret_cast<const char*>(input.getData()), input.len, result,
        LogicalType{LogicalTypeID::INT32});
}

template<>
inline void CastString::operation(const ku_string_t& input, int16_t& result,
    ValueVector* /*resultVector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    simpleIntegerCast<int16_t>(reinterpret_cast<const char*>(input.getData()), input.len, result,
        LogicalType{LogicalTypeID::INT16});
}

template<>
inline void CastString::operation(const ku_string_t& input, int8_t& result,
    ValueVector* /*resultVector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    simpleIntegerCast<int8_t>(reinterpret_cast<const char*>(input.getData()), input.len, result,
        LogicalType{LogicalTypeID::INT8});
}

template<>
inline void CastString::operation(const ku_string_t& input, uint64_t& result,
    ValueVector* /*resultVector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    simpleIntegerCast<uint64_t, false>(reinterpret_cast<const char*>(input.getData()), input.len,
        result, LogicalType{LogicalTypeID::UINT64});
}

template<>
inline void CastString::operation(const ku_string_t& input, uint32_t& result,
    ValueVector* /*resultVector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    simpleIntegerCast<uint32_t, false>(reinterpret_cast<const char*>(input.getData()), input.len,
        result, LogicalType{LogicalTypeID::UINT32});
}

template<>
inline void CastString::operation(const ku_string_t& input, uint16_t& result,
    ValueVector* /*resultVector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    simpleIntegerCast<uint16_t, false>(reinterpret_cast<const char*>(input.getData()), input.len,
        result, LogicalType{LogicalTypeID::UINT16});
}

template<>
inline void CastString::operation(const ku_string_t& input, uint8_t& result,
    ValueVector* /*resultVector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    simpleIntegerCast<uint8_t, false>(reinterpret_cast<const char*>(input.getData()), input.len,
        result, LogicalType{LogicalTypeID::UINT8});
}

template<>
inline void CastString::operation(const ku_string_t& input, float_t& result,
    ValueVector* /*resultVector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    doubleCast<float_t>(reinterpret_cast<const char*>(input.getData()), input.len, result,
        LogicalType{LogicalTypeID::FLOAT});
}

template<>
inline void CastString::operation(const ku_string_t& input, double_t& result,
    ValueVector* /*resultVector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    doubleCast<double_t>(reinterpret_cast<const char*>(input.getData()), input.len, result,
        LogicalType{LogicalTypeID::DOUBLE});
}

template<>
inline void CastString::operation(const ku_string_t& input, date_t& result,
    ValueVector* /*resultVector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    result = Date::fromCString((const char*)input.getData(), input.len);
}

template<>
inline void CastString::operation(const ku_string_t& input, timestamp_t& result,
    ValueVector* /*resultVector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    result = Timestamp::fromCString((const char*)input.getData(), input.len);
}

template<>
inline void CastString::operation(const ku_string_t& input, interval_t& result,
    ValueVector* /*resultVector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    result = Interval::fromCString((const char*)input.getData(), input.len);
}

template<>
inline void CastString::operation(const ku_string_t& input, bool& result,
    ValueVector* /*resultVector*/, uint64_t /*rowToAdd*/, const CSVOption* /*option*/) {
    castStringToBool(reinterpret_cast<const char*>(input.getData()), input.len, result);
}

template<>
void CastString::operation(const ku_string_t& input, blob_t& result, ValueVector* resultVector,
    uint64_t rowToAdd, const CSVOption* option);

template<>
void CastString::operation(const ku_string_t& input, list_entry_t& result,
    ValueVector* resultVector, uint64_t rowToAdd, const CSVOption* option);

template<>
void CastString::operation(const ku_string_t& input, map_entry_t& result, ValueVector* resultVector,
    uint64_t rowToAdd, const CSVOption* option);

template<>
void CastString::operation(const ku_string_t& input, struct_entry_t& result,
    ValueVector* resultVector, uint64_t rowToAdd, const CSVOption* option);

template<>
void CastString::operation(const ku_string_t& input, union_entry_t& result,
    ValueVector* resultVector, uint64_t rowToAdd, const CSVOption* option);

} // namespace function
} // namespace kuzu
