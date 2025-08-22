#include "common/exception/runtime.h"
#include "common/type_utils.h"
#include "common/types/types.h"

namespace kuzu {
namespace function {

struct WeightUtils
{
    template<typename... Fs>
    static auto visit(const common::LogicalType& dataType, Fs... funcs);

    template<typename... Fs>
    static auto visit(const common::LogicalTypeID& dataType, Fs... funcs);

    template<typename T>
    static void checkWeight(T weight);
};


template<typename... Fs>
auto WeightUtils::visit(const common::LogicalType& dataType, Fs... funcs) {
    auto func = common::overload(funcs...);
    switch (dataType.getLogicalTypeID()) {
    /* NOLINTBEGIN(bugprone-branch-clone)*/
    case common::LogicalTypeID::INT8:
        return func(int8_t());
    case common::LogicalTypeID::UINT8:
        return func(uint8_t());
    case common::LogicalTypeID::INT16:
        return func(int16_t());
    case common::LogicalTypeID::UINT16:
        return func(uint16_t());
    case common::LogicalTypeID::INT32:
        return func(int32_t());
    case common::LogicalTypeID::UINT32:
        return func(uint32_t());
    case common::LogicalTypeID::INT64:
        return func(int64_t());
    case common::LogicalTypeID::UINT64:
        return func(uint64_t());
    case common::LogicalTypeID::DOUBLE:
        return func(double());
    case common::LogicalTypeID::FLOAT:
        return func(float());
    /* NOLINTEND(bugprone-branch-clone)*/
    default:
        break;
    }
    // LCOV_EXCL_START
    throw common::RuntimeException(common::stringFormat(
        "{} weight type is not supported for weighted shortest path.", dataType.toString()));
    // LCOV_EXCL_STOP
}

template<typename... Fs>
auto WeightUtils::visit(const common::LogicalTypeID& dataType, Fs... funcs) {
    auto func = common::overload(funcs...);
    switch (dataType) {
    /* NOLINTBEGIN(bugprone-branch-clone)*/
    case common::LogicalTypeID::INT8:
        return func(int8_t());
    case common::LogicalTypeID::UINT8:
        return func(uint8_t());
    case common::LogicalTypeID::INT16:
        return func(int16_t());
    case common::LogicalTypeID::UINT16:
        return func(uint16_t());
    case common::LogicalTypeID::INT32:
        return func(int32_t());
    case common::LogicalTypeID::UINT32:
        return func(uint32_t());
    case common::LogicalTypeID::INT64:
        return func(int64_t());
    case common::LogicalTypeID::UINT64:
        return func(uint64_t());
    case common::LogicalTypeID::DOUBLE:
        return func(double());
    case common::LogicalTypeID::FLOAT:
        return func(float());
    /* NOLINTEND(bugprone-branch-clone)*/
    default:
        break;
    }
    // LCOV_EXCL_START
    throw common::RuntimeException(common::stringFormat(
        "{} weight type is not supported for weighted shortest path.", common::LogicalType(dataType).toString()));
    // LCOV_EXCL_STOP
}

template<typename T>
void WeightUtils::checkWeight(T weight) {
    if (weight < 0) {
        [[unlikely]] throw common::RuntimeException(common::stringFormat(
            "Found negative weight {}. This is not a supported weight.", weight));
    }
}

} // namespace function
} // namespace kuzu
