#include "storage/predicate/constant_predicate.h"

#include "common/type_utils.h"
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
    auto physicalType = value.getDataType().getPhysicalType();
    return TypeUtils::visit(
        physicalType,
        [&]<StorageValueType T>(
            T) { return checkZoneMapSwitch<T>(metadata, expressionType, value); },
        [&](auto) { return ZoneMapCheckResult::ALWAYS_SCAN; });
}

} // namespace storage
} // namespace kuzu
