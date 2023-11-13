#include "function/aggregate_function.h"

#include "common/types/interval_t.h"
#include "function/aggregate/avg.h"
#include "function/aggregate/min_max.h"
#include "function/aggregate/sum.h"
#include "function/comparison/comparison_functions.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace function {

std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getSumFunc(const std::string name,
    common::LogicalTypeID inputType, common::LogicalTypeID resultType, bool isDistinct) {
    switch (inputType) {
    case common::LogicalTypeID::INT128:
        return getAggFunc<SumFunction<int128_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    case common::LogicalTypeID::SERIAL:
    case common::LogicalTypeID::INT64:
        return getAggFunc<SumFunction<int64_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    case common::LogicalTypeID::INT32:
        return getAggFunc<SumFunction<int32_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    case common::LogicalTypeID::INT16:
        return getAggFunc<SumFunction<int16_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    case common::LogicalTypeID::INT8:
        return getAggFunc<SumFunction<int8_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    case common::LogicalTypeID::UINT64:
        return getAggFunc<SumFunction<uint64_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    case common::LogicalTypeID::UINT32:
        return getAggFunc<SumFunction<uint32_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    case common::LogicalTypeID::UINT16:
        return getAggFunc<SumFunction<uint16_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    case common::LogicalTypeID::UINT8:
        return getAggFunc<SumFunction<uint8_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    case common::LogicalTypeID::DOUBLE:
        return getAggFunc<SumFunction<double_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    case common::LogicalTypeID::FLOAT:
        return getAggFunc<SumFunction<float_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    default:
        KU_UNREACHABLE;
    }
}

std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getAvgFunc(const std::string name,
    common::LogicalTypeID inputType, common::LogicalTypeID resultType, bool isDistinct) {
    switch (inputType) {
    case common::LogicalTypeID::INT128:
        return getAggFunc<AvgFunction<int128_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    case common::LogicalTypeID::SERIAL:
    case common::LogicalTypeID::INT64:
        return getAggFunc<AvgFunction<int64_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    case common::LogicalTypeID::INT32:
        return getAggFunc<AvgFunction<int32_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    case common::LogicalTypeID::INT16:
        return getAggFunc<AvgFunction<int16_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    case common::LogicalTypeID::INT8:
        return getAggFunc<AvgFunction<int8_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    case common::LogicalTypeID::UINT64:
        return getAggFunc<AvgFunction<uint64_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    case common::LogicalTypeID::UINT32:
        return getAggFunc<AvgFunction<uint32_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    case common::LogicalTypeID::UINT16:
        return getAggFunc<AvgFunction<uint16_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    case common::LogicalTypeID::UINT8:
        return getAggFunc<AvgFunction<uint8_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    case common::LogicalTypeID::DOUBLE:
        return getAggFunc<AvgFunction<double_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    case common::LogicalTypeID::FLOAT:
        return getAggFunc<AvgFunction<float_t>>(
            name, std::move(inputType), std::move(resultType), isDistinct);
    default:
        KU_UNREACHABLE;
    }
}

std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getMinFunc(
    const LogicalType& inputType, bool isDistinct) {
    return AggregateFunctionUtil::getMinMaxFunction<LessThan>(
        MIN_FUNC_NAME, inputType, inputType.getLogicalTypeID(), isDistinct);
}

std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getMaxFunc(
    const LogicalType& inputType, bool isDistinct) {
    return AggregateFunctionUtil::getMinMaxFunction<GreaterThan>(
        MAX_FUNC_NAME, inputType, inputType.getLogicalTypeID(), isDistinct);
}

template<typename FUNC>
std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getMinMaxFunction(const std::string name,
    const common::LogicalType& inputType, common::LogicalTypeID resultType, bool isDistinct) {
    auto inputTypes = std::vector<common::LogicalTypeID>{inputType.getLogicalTypeID()};
    switch (inputType.getPhysicalType()) {
    case PhysicalTypeID::BOOL:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            std::move(resultType), MinMaxFunction<bool>::initialize,
            MinMaxFunction<bool>::updateAll<FUNC>, MinMaxFunction<bool>::updatePos<FUNC>,
            MinMaxFunction<bool>::combine<FUNC>, MinMaxFunction<bool>::finalize, isDistinct);
    case PhysicalTypeID::INT128:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            std::move(resultType), MinMaxFunction<int128_t>::initialize,
            MinMaxFunction<int128_t>::updateAll<FUNC>, MinMaxFunction<int128_t>::updatePos<FUNC>,
            MinMaxFunction<int128_t>::combine<FUNC>, MinMaxFunction<int128_t>::finalize,
            isDistinct);
    case PhysicalTypeID::INT64:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            std::move(resultType), MinMaxFunction<int64_t>::initialize,
            MinMaxFunction<int64_t>::updateAll<FUNC>, MinMaxFunction<int64_t>::updatePos<FUNC>,
            MinMaxFunction<int64_t>::combine<FUNC>, MinMaxFunction<int64_t>::finalize, isDistinct);
    case PhysicalTypeID::INT32:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            std::move(resultType), MinMaxFunction<int32_t>::initialize,
            MinMaxFunction<int32_t>::updateAll<FUNC>, MinMaxFunction<int32_t>::updatePos<FUNC>,
            MinMaxFunction<int32_t>::combine<FUNC>, MinMaxFunction<int32_t>::finalize, isDistinct);
    case PhysicalTypeID::INT16:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            std::move(resultType), MinMaxFunction<int16_t>::initialize,
            MinMaxFunction<int16_t>::updateAll<FUNC>, MinMaxFunction<int16_t>::updatePos<FUNC>,
            MinMaxFunction<int16_t>::combine<FUNC>, MinMaxFunction<int16_t>::finalize, isDistinct);
    case PhysicalTypeID::INT8:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            std::move(resultType), MinMaxFunction<int8_t>::initialize,
            MinMaxFunction<int8_t>::updateAll<FUNC>, MinMaxFunction<int8_t>::updatePos<FUNC>,
            MinMaxFunction<int8_t>::combine<FUNC>, MinMaxFunction<int8_t>::finalize, isDistinct);
    case PhysicalTypeID::UINT64:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            std::move(resultType), MinMaxFunction<uint64_t>::initialize,
            MinMaxFunction<uint64_t>::updateAll<FUNC>, MinMaxFunction<uint64_t>::updatePos<FUNC>,
            MinMaxFunction<uint64_t>::combine<FUNC>, MinMaxFunction<uint64_t>::finalize,
            isDistinct);
    case PhysicalTypeID::UINT32:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            std::move(resultType), MinMaxFunction<uint32_t>::initialize,
            MinMaxFunction<uint32_t>::updateAll<FUNC>, MinMaxFunction<uint32_t>::updatePos<FUNC>,
            MinMaxFunction<uint32_t>::combine<FUNC>, MinMaxFunction<uint32_t>::finalize,
            isDistinct);
    case PhysicalTypeID::UINT16:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            std::move(resultType), MinMaxFunction<uint16_t>::initialize,
            MinMaxFunction<uint16_t>::updateAll<FUNC>, MinMaxFunction<uint16_t>::updatePos<FUNC>,
            MinMaxFunction<uint16_t>::combine<FUNC>, MinMaxFunction<uint16_t>::finalize,
            isDistinct);
    case PhysicalTypeID::UINT8:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            std::move(resultType), MinMaxFunction<uint8_t>::initialize,
            MinMaxFunction<uint8_t>::updateAll<FUNC>, MinMaxFunction<uint8_t>::updatePos<FUNC>,
            MinMaxFunction<uint8_t>::combine<FUNC>, MinMaxFunction<uint8_t>::finalize, isDistinct);
    case PhysicalTypeID::DOUBLE:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            std::move(resultType), MinMaxFunction<double_t>::initialize,
            MinMaxFunction<double_t>::updateAll<FUNC>, MinMaxFunction<double_t>::updatePos<FUNC>,
            MinMaxFunction<double_t>::combine<FUNC>, MinMaxFunction<double_t>::finalize,
            isDistinct);
    case PhysicalTypeID::FLOAT:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            std::move(resultType), MinMaxFunction<float_t>::initialize,
            MinMaxFunction<float_t>::updateAll<FUNC>, MinMaxFunction<float_t>::updatePos<FUNC>,
            MinMaxFunction<float_t>::combine<FUNC>, MinMaxFunction<float_t>::finalize, isDistinct);
    case PhysicalTypeID::INTERVAL:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            std::move(resultType), MinMaxFunction<interval_t>::initialize,
            MinMaxFunction<interval_t>::updateAll<FUNC>,
            MinMaxFunction<interval_t>::updatePos<FUNC>, MinMaxFunction<interval_t>::combine<FUNC>,
            MinMaxFunction<interval_t>::finalize, isDistinct);
    case PhysicalTypeID::STRING:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            std::move(resultType), MinMaxFunction<ku_string_t>::initialize,
            MinMaxFunction<ku_string_t>::updateAll<FUNC>,
            MinMaxFunction<ku_string_t>::updatePos<FUNC>,
            MinMaxFunction<ku_string_t>::combine<FUNC>, MinMaxFunction<ku_string_t>::finalize,
            isDistinct);
    case PhysicalTypeID::INTERNAL_ID:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            std::move(resultType), MinMaxFunction<internalID_t>::initialize,
            MinMaxFunction<internalID_t>::updateAll<FUNC>,
            MinMaxFunction<internalID_t>::updatePos<FUNC>,
            MinMaxFunction<internalID_t>::combine<FUNC>, MinMaxFunction<internalID_t>::finalize,
            isDistinct);
    default:
        KU_UNREACHABLE;
    }
}

} // namespace function
} // namespace kuzu
