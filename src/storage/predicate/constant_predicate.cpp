#include "storage/predicate/constant_predicate.h"

#include "common/type_utils.h"
#include "function/comparison/comparison_functions.h"
#include "storage/compression/compression.h"
#include "storage/store/column_chunk_stats.h"

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
ZoneMapCheckResult checkZoneMapSwitch(const ColumnChunkStats& stats, ExpressionType expressionType,
    const Value& value) {
    KU_ASSERT(stats.min.has_value() && stats.max.has_value());
    auto max = stats.max->get<T>();
    auto min = stats.min->get<T>();
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

ZoneMapCheckResult ColumnConstantPredicate::checkZoneMap(const ColumnChunkStats& stats) const {
    auto physicalType = value.getDataType().getPhysicalType();
    return TypeUtils::visit(
        physicalType,
        [&]<StorageValueType T>(T) { return checkZoneMapSwitch<T>(stats, expressionType, value); },
        [&](auto) { return ZoneMapCheckResult::ALWAYS_SCAN; });
}

std::string ColumnConstantPredicate::toString() {
    std::string valStr;
    if (value.getDataType().getPhysicalType() == PhysicalTypeID::STRING ||
        value.getDataType().getPhysicalType() == PhysicalTypeID::LIST ||
        value.getDataType().getPhysicalType() == PhysicalTypeID::ARRAY ||
        value.getDataType().getPhysicalType() == PhysicalTypeID::STRUCT ||
        value.getDataType().getLogicalTypeID() == LogicalTypeID::UUID ||
        value.getDataType().getLogicalTypeID() == LogicalTypeID::TIMESTAMP ||
        value.getDataType().getLogicalTypeID() == LogicalTypeID::DATE ||
        value.getDataType().getLogicalTypeID() == LogicalTypeID::INTERVAL) {
        valStr = stringFormat("'{}'", value.toString());
    } else {
        valStr = value.toString();
    }
    return stringFormat("{} {} {}", columnName,
        ExpressionTypeUtil::toParsableString(expressionType), valStr);
}

} // namespace storage
} // namespace kuzu
