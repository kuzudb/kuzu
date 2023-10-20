#pragma once

#include <cassert>

#include "cast_string_non_nested_functions.h"
#include "common/copier_config/copier_config.h"
#include "common/exception/runtime.h"
#include "common/string_format.h"
#include "common/type_utils.h"
#include "common/types/blob.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct CastStringToTypes {
    static void copyStringToVector(common::ValueVector* vector, uint64_t rowToAdd,
        std::string_view strVal, const common::CSVReaderConfig* csvReaderConfig);

    template<typename T>
    static inline bool tryCast(const char* input, uint64_t len, T& result) {
        // try cast for signed integer types
        return trySimpleIntegerCast<T, true>(input, len, result);
    }

    template<typename T>
    static inline void operation(
        common::ku_string_t& input, T& result, common::ValueVector& /*resultVector*/) {
        // base case: int64
        simpleIntegerCast<T, true>((char*)input.getData(), input.len, result,
            common::LogicalType{common::LogicalTypeID::INT64});
    }

    template<typename T>
    static void operation(const char* input, uint64_t len, T& result,
        common::ValueVector* /*vector*/ = nullptr, uint64_t /*rowToAdd*/ = 0,
        const common::CSVReaderConfig* /*csvReaderConfig*/ = nullptr) {
        simpleIntegerCast<int64_t>(
            input, len, result, common::LogicalType{common::LogicalTypeID::INT64});
    }
};

template<>
inline void CastStringToTypes::operation(
    common::ku_string_t& input, common::int128_t& result, common::ValueVector& /*resultVector*/) {
    simpleInt128Cast((char*)input.getData(), input.len, result);
}

template<>
inline void CastStringToTypes::operation(
    common::ku_string_t& input, int32_t& result, common::ValueVector& /*resultVector*/) {
    simpleIntegerCast<int32_t>((char*)input.getData(), input.len, result,
        common::LogicalType{common::LogicalTypeID::INT32});
}

template<>
inline void CastStringToTypes::operation(
    common::ku_string_t& input, int16_t& result, common::ValueVector& /*resultVector*/) {
    simpleIntegerCast<int16_t>((char*)input.getData(), input.len, result,
        common::LogicalType{common::LogicalTypeID::INT16});
}

template<>
inline void CastStringToTypes::operation(
    common::ku_string_t& input, int8_t& result, common::ValueVector& /*resultVector*/) {
    simpleIntegerCast<int8_t>((char*)input.getData(), input.len, result,
        common::LogicalType{common::LogicalTypeID::INT8});
}

template<>
inline void CastStringToTypes::operation(
    common::ku_string_t& input, uint64_t& result, common::ValueVector& /*resultVector*/) {
    simpleIntegerCast<uint64_t, false>((char*)input.getData(), input.len, result,
        common::LogicalType{common::LogicalTypeID::UINT64});
}

template<>
inline void CastStringToTypes::operation(
    common::ku_string_t& input, uint32_t& result, common::ValueVector& /*resultVector*/) {
    simpleIntegerCast<uint32_t, false>((char*)input.getData(), input.len, result,
        common::LogicalType{common::LogicalTypeID::UINT32});
}

template<>
inline void CastStringToTypes::operation(
    common::ku_string_t& input, uint16_t& result, common::ValueVector& /*resultVector*/) {
    simpleIntegerCast<uint16_t, false>((char*)input.getData(), input.len, result,
        common::LogicalType{common::LogicalTypeID::UINT16});
}

template<>
inline void CastStringToTypes::operation(
    common::ku_string_t& input, uint8_t& result, common::ValueVector& /*resultVector*/) {
    simpleIntegerCast<uint8_t, false>((char*)input.getData(), input.len, result,
        common::LogicalType{common::LogicalTypeID::UINT8});
}

template<>
inline void CastStringToTypes::operation(
    common::ku_string_t& input, float_t& result, common::ValueVector& /*resultVector*/) {
    doubleCast<float_t>((char*)input.getData(), input.len, result,
        common::LogicalType{common::LogicalTypeID::FLOAT});
}

template<>
inline void CastStringToTypes::operation(
    common::ku_string_t& input, double_t& result, common::ValueVector& /*resultVector*/) {
    doubleCast<double_t>((char*)input.getData(), input.len, result,
        common::LogicalType{common::LogicalTypeID::DOUBLE});
}

template<>
inline void CastStringToTypes::operation(
    common::ku_string_t& input, common::date_t& result, common::ValueVector& /*resultVector*/) {
    result = common::Date::fromCString((const char*)input.getData(), input.len);
}

template<>
inline void CastStringToTypes::operation(common::ku_string_t& input, common::timestamp_t& result,
    common::ValueVector& /*resultVector*/) {
    result = common::Timestamp::fromCString((const char*)input.getData(), input.len);
}

template<>
inline void CastStringToTypes::operation(
    common::ku_string_t& input, common::interval_t& result, common::ValueVector& /*resultVector*/) {
    result = common::Interval::fromCString((const char*)input.getData(), input.len);
}

template<>
inline void CastStringToTypes::operation(
    common::ku_string_t& input, bool& result, common::ValueVector& /*resultVector*/) {
    castStringToBool(reinterpret_cast<const char*>(input.getData()), input.len, result);
}

template<>
void CastStringToTypes::operation(
    common::ku_string_t& input, common::blob_t& result, common::ValueVector& resultVector);

template<>
void CastStringToTypes::operation(
    common::ku_string_t& input, common::list_entry_t& result, common::ValueVector& resultVector);

template<>
void CastStringToTypes::operation(
    common::ku_string_t& input, common::map_entry_t& result, common::ValueVector& resultVector);

template<>
void CastStringToTypes::operation(
    common::ku_string_t& input, common::union_entry_t& result, common::ValueVector& resultVector);

template<>
void CastStringToTypes::operation(
    common::ku_string_t& input, common::struct_entry_t& result, common::ValueVector& resultVector);

template<>
inline void CastStringToTypes::operation(const char* input, uint64_t len, common::int128_t& result,
    common::ValueVector* /*vector*/, uint64_t /*rowToAdd*/,
    const common::CSVReaderConfig* /*csvReaderConfig*/) {
    simpleInt128Cast(input, len, result);
}

template<>
inline void CastStringToTypes::operation(const char* input, uint64_t len, int32_t& result,
    common::ValueVector* /*vector*/, uint64_t /*rowToAdd*/,
    const common::CSVReaderConfig* /*csvReaderConfig*/) {
    simpleIntegerCast<int32_t>(
        input, len, result, common::LogicalType{common::LogicalTypeID::INT32});
}

template<>
inline void CastStringToTypes::operation(const char* input, uint64_t len, int16_t& result,
    common::ValueVector* /*vector*/, uint64_t /*rowToAdd*/,
    const common::CSVReaderConfig* /*csvReaderConfig*/) {
    simpleIntegerCast<int16_t>(
        input, len, result, common::LogicalType{common::LogicalTypeID::INT16});
}

template<>
inline void CastStringToTypes::operation(const char* input, uint64_t len, int8_t& result,
    common::ValueVector* /*vector*/, uint64_t /*rowToAdd*/,
    const common::CSVReaderConfig* /*csvReaderConfig*/) {
    simpleIntegerCast<int8_t>(input, len, result, common::LogicalType{common::LogicalTypeID::INT8});
}

template<>
inline void CastStringToTypes::operation(const char* input, uint64_t len, uint64_t& result,
    common::ValueVector* /*vector*/, uint64_t /*rowToAdd*/,
    const common::CSVReaderConfig* /*csvReaderConfig*/) {
    simpleIntegerCast<uint64_t, false>(
        input, len, result, common::LogicalType{common::LogicalTypeID::UINT64});
}

template<>
inline void CastStringToTypes::operation(const char* input, uint64_t len, uint32_t& result,
    common::ValueVector* /*vector*/, uint64_t /*rowToAdd*/,
    const common::CSVReaderConfig* /*csvReaderConfig*/) {
    simpleIntegerCast<uint32_t, false>(
        input, len, result, common::LogicalType{common::LogicalTypeID::UINT32});
}

template<>
inline void CastStringToTypes::operation(const char* input, uint64_t len, uint16_t& result,
    common::ValueVector* /*vector*/, uint64_t /*rowToAdd*/,
    const common::CSVReaderConfig* /*csvReaderConfig*/) {
    simpleIntegerCast<uint16_t, false>(
        input, len, result, common::LogicalType{common::LogicalTypeID::UINT16});
}

template<>
inline void CastStringToTypes::operation(const char* input, uint64_t len, uint8_t& result,
    common::ValueVector* /*vector*/, uint64_t /*rowToAdd*/,
    const common::CSVReaderConfig* /*csvReaderConfig*/) {
    simpleIntegerCast<uint8_t, false>(
        input, len, result, common::LogicalType{common::LogicalTypeID::UINT8});
}

template<>
inline void CastStringToTypes::operation(const char* input, uint64_t len, float_t& result,
    common::ValueVector* /*vector*/, uint64_t /*rowToAdd*/,
    const common::CSVReaderConfig* /*csvReaderConfig*/) {
    doubleCast<float_t>(input, len, result, common::LogicalType{common::LogicalTypeID::FLOAT});
}

template<>
inline void CastStringToTypes::operation(const char* input, uint64_t len, double_t& result,
    common::ValueVector* /*vector*/, uint64_t /*rowToAdd*/,
    const common::CSVReaderConfig* /*csvReaderConfig*/) {
    doubleCast<double_t>(input, len, result, common::LogicalType{common::LogicalTypeID::DOUBLE});
}

template<>
inline void CastStringToTypes::operation(const char* input, uint64_t len, bool& result,
    common::ValueVector* /*vector*/, uint64_t /*rowToAdd*/,
    const common::CSVReaderConfig* /*csvReaderConfig*/) {
    castStringToBool(input, len, result);
}

template<>
inline void CastStringToTypes::operation(const char* input, uint64_t len, common::date_t& result,
    common::ValueVector* /*vector*/, uint64_t /*rowToAdd*/,
    const common::CSVReaderConfig* /*csvReaderConfig*/) {
    result = common::Date::fromCString(input, len);
}

template<>
inline void CastStringToTypes::operation(const char* input, uint64_t len,
    common::timestamp_t& result, common::ValueVector* /*vector*/, uint64_t /*rowToAdd*/,
    const common::CSVReaderConfig* /*csvReaderConfig*/) {
    result = common::Timestamp::fromCString(input, len);
}

template<>
inline void CastStringToTypes::operation(const char* input, uint64_t len,
    common::interval_t& result, common::ValueVector* /*vector*/, uint64_t /*rowToAdd*/,
    const common::CSVReaderConfig* /*csvReaderConfig*/) {
    result = common::Interval::fromCString(input, len);
}

template<>
void CastStringToTypes::operation(const char* input, uint64_t len, common::blob_t& result,
    common::ValueVector* vector, uint64_t rowToAdd, const common::CSVReaderConfig* csvReaderConfig);

template<>
void CastStringToTypes::operation<common::list_entry_t>(const char* input, uint64_t len,
    common::list_entry_t& result, common::ValueVector* vector, uint64_t rowToAdd,
    const common::CSVReaderConfig* csvReaderConfig);

template<>
void CastStringToTypes::operation<common::map_entry_t>(const char* input, uint64_t len,
    common::map_entry_t& result, common::ValueVector* vector, uint64_t rowToAdd,
    const common::CSVReaderConfig* csvReaderConfig);

template<>
void CastStringToTypes::operation<common::struct_entry_t>(const char* input, uint64_t len,
    common::struct_entry_t& result, common::ValueVector* vector, uint64_t rowToAdd,
    const common::CSVReaderConfig* csvReaderConfig);

template<>
void CastStringToTypes::operation<common::union_entry_t>(const char* input, uint64_t len,
    common::union_entry_t& result, common::ValueVector* vector, uint64_t rowToAdd,
    const common::CSVReaderConfig* csvReaderConfig);

} // namespace function
} // namespace kuzu
