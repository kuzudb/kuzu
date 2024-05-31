#include "storage/predicate/constant_predicate.h"

#include "function/comparison/comparison_functions.h"
#include "storage/compression/compression.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace storage {

template<typename T>
bool inRange(T min, T max, T val) {
    auto a = GreaterThanEquals::operation<T>(val, min);
    auto b = LessThanEquals::operation<T>(val, max);
    return a && b;
}

template<typename T>
ZoneMapCheckResult checkZoneMapSwitch(const CompressionMetadata& metadata,
    ExpressionType expressionType, const Value& value) {
    auto max = metadata.max.get<T>();
    auto min = metadata.min.get<T>();
    auto constant = value.getValue<T>();
    switch (expressionType) {
    case ExpressionType::EQUALS: {
        if (!inRange<T>(min, max, constant)) {
            return ZoneMapCheckResult::SKIP_SCAN;
        }
    } break;
    case ExpressionType::NOT_EQUALS: {
        if (Equals::operation<T>(constant, min) && Equals::operation<T>(constant, max)) {
            return ZoneMapCheckResult::SKIP_SCAN;
        }
    } break;
    case ExpressionType::GREATER_THAN: {
        if (GreaterThanEquals::operation<T>(constant, max)) {
            return ZoneMapCheckResult::SKIP_SCAN;
        }
    } break;
    case ExpressionType::GREATER_THAN_EQUALS: {
        if (GreaterThan::operation<T>(constant, max)) {
            return ZoneMapCheckResult::SKIP_SCAN;
        }
    } break;
    case ExpressionType::LESS_THAN: {
        if (LessThanEquals::operation<T>(constant, min)) {
            return ZoneMapCheckResult::SKIP_SCAN;
        }
    } break;
    case ExpressionType::LESS_THAN_EQUALS: {
        if (LessThan::operation<T>(constant, min)) {
            return ZoneMapCheckResult::SKIP_SCAN;
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
    return ZoneMapCheckResult::ALWAYS_SCAN;
}

ZoneMapCheckResult ColumnConstantPredicate::checkZoneMap(
    const CompressionMetadata& metadata) const {
    auto physicalType = value.getDataType()->getPhysicalType();
    switch (physicalType) {
    case PhysicalTypeID::INT8:
        return checkZoneMapSwitch<int8_t>(metadata, expressionType, value);
    case PhysicalTypeID::INT16:
        return checkZoneMapSwitch<int16_t>(metadata, expressionType, value);
    case PhysicalTypeID::INT32:
        return checkZoneMapSwitch<int32_t>(metadata, expressionType, value);
    case PhysicalTypeID::INT64:
        return checkZoneMapSwitch<int64_t>(metadata, expressionType, value);
    case PhysicalTypeID::UINT8:
        return checkZoneMapSwitch<uint8_t>(metadata, expressionType, value);
    case PhysicalTypeID::UINT16:
        return checkZoneMapSwitch<uint16_t>(metadata, expressionType, value);
    case PhysicalTypeID::UINT32:
        return checkZoneMapSwitch<uint32_t>(metadata, expressionType, value);
    case PhysicalTypeID::UINT64:
        return checkZoneMapSwitch<uint64_t>(metadata, expressionType, value);
    case PhysicalTypeID::FLOAT:
        return checkZoneMapSwitch<float>(metadata, expressionType, value);
    case PhysicalTypeID::DOUBLE:
        return checkZoneMapSwitch<double>(metadata, expressionType, value);
    default:
        return ZoneMapCheckResult::ALWAYS_SCAN;
    }
}

} // namespace storage
} // namespace kuzu
