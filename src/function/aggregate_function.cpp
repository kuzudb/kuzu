#include "function/aggregate_function.h"

#include "common/type_utils.h"
#include "common/types/interval_t.h"
#include "function/aggregate/avg.h"
#include "function/aggregate/min_max.h"
#include "function/aggregate/sum.h"
#include "function/comparison/comparison_functions.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace function {

static void appendSumFuncs(std::string name, common::LogicalTypeID inputType,
    function_set& result) {
    std::unique_ptr<AggregateFunction> aggFunc;
    for (auto isDistinct : std::vector<bool>{true, false}) {
        TypeUtils::visit(
            LogicalType{inputType},
            [&]<IntegerTypes T>(T) {
                aggFunc = AggregateFunctionUtil::getAggFunc<SumFunction<T, int128_t>>(name,
                    inputType, LogicalTypeID::INT128, isDistinct);
            },
            [&]<FloatingPointTypes T>(T) {
                aggFunc = AggregateFunctionUtil::getAggFunc<SumFunction<T, double>>(name, inputType,
                    LogicalTypeID::DOUBLE, isDistinct);
            },
            [](auto) { KU_UNREACHABLE; });
        result.push_back(std::move(aggFunc));
    }
}

static void appendAvgFuncs(std::string name, common::LogicalTypeID inputType,
    function_set& result) {
    std::unique_ptr<AggregateFunction> aggFunc;
    for (auto isDistinct : std::vector<bool>{true, false}) {
        TypeUtils::visit(
            LogicalType{inputType},
            [&]<IntegerTypes T>(T) {
                aggFunc = AggregateFunctionUtil::getAggFunc<AvgFunction<T, int128_t>>(name,
                    inputType, LogicalTypeID::DOUBLE, isDistinct);
            },
            [&]<FloatingPointTypes T>(T) {
                aggFunc = AggregateFunctionUtil::getAggFunc<AvgFunction<T, double>>(name, inputType,
                    LogicalTypeID::DOUBLE, isDistinct);
            },
            [](auto) { KU_UNREACHABLE; });
        result.push_back(std::move(aggFunc));
    }
}

std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getMinFunc(LogicalTypeID inputType,
    bool isDistinct) {
    return AggregateFunctionUtil::getMinMaxFunction<LessThan>(AggregateMinFunction::name, inputType,
        inputType, isDistinct);
}

std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getMaxFunc(LogicalTypeID inputType,
    bool isDistinct) {
    return AggregateFunctionUtil::getMinMaxFunction<GreaterThan>(AggregateMaxFunction::name,
        inputType, inputType, isDistinct);
}

template<typename FUNC>
std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getMinMaxFunction(std::string name,
    common::LogicalTypeID inputType, common::LogicalTypeID resultType, bool isDistinct) {
    auto inputTypes = std::vector<common::LogicalTypeID>{inputType};
    switch (LogicalType::getPhysicalType(inputType)) {
    case PhysicalTypeID::BOOL:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            resultType, MinMaxFunction<bool>::initialize, MinMaxFunction<bool>::updateAll<FUNC>,
            MinMaxFunction<bool>::updatePos<FUNC>, MinMaxFunction<bool>::combine<FUNC>,
            MinMaxFunction<bool>::finalize, isDistinct);
    case PhysicalTypeID::INT128:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            resultType, MinMaxFunction<int128_t>::initialize,
            MinMaxFunction<int128_t>::updateAll<FUNC>, MinMaxFunction<int128_t>::updatePos<FUNC>,
            MinMaxFunction<int128_t>::combine<FUNC>, MinMaxFunction<int128_t>::finalize,
            isDistinct);
    case PhysicalTypeID::INT64:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            resultType, MinMaxFunction<int64_t>::initialize,
            MinMaxFunction<int64_t>::updateAll<FUNC>, MinMaxFunction<int64_t>::updatePos<FUNC>,
            MinMaxFunction<int64_t>::combine<FUNC>, MinMaxFunction<int64_t>::finalize, isDistinct);
    case PhysicalTypeID::INT32:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            resultType, MinMaxFunction<int32_t>::initialize,
            MinMaxFunction<int32_t>::updateAll<FUNC>, MinMaxFunction<int32_t>::updatePos<FUNC>,
            MinMaxFunction<int32_t>::combine<FUNC>, MinMaxFunction<int32_t>::finalize, isDistinct);
    case PhysicalTypeID::INT16:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            resultType, MinMaxFunction<int16_t>::initialize,
            MinMaxFunction<int16_t>::updateAll<FUNC>, MinMaxFunction<int16_t>::updatePos<FUNC>,
            MinMaxFunction<int16_t>::combine<FUNC>, MinMaxFunction<int16_t>::finalize, isDistinct);
    case PhysicalTypeID::INT8:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            resultType, MinMaxFunction<int8_t>::initialize, MinMaxFunction<int8_t>::updateAll<FUNC>,
            MinMaxFunction<int8_t>::updatePos<FUNC>, MinMaxFunction<int8_t>::combine<FUNC>,
            MinMaxFunction<int8_t>::finalize, isDistinct);
    case PhysicalTypeID::UINT64:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            resultType, MinMaxFunction<uint64_t>::initialize,
            MinMaxFunction<uint64_t>::updateAll<FUNC>, MinMaxFunction<uint64_t>::updatePos<FUNC>,
            MinMaxFunction<uint64_t>::combine<FUNC>, MinMaxFunction<uint64_t>::finalize,
            isDistinct);
    case PhysicalTypeID::UINT32:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            resultType, MinMaxFunction<uint32_t>::initialize,
            MinMaxFunction<uint32_t>::updateAll<FUNC>, MinMaxFunction<uint32_t>::updatePos<FUNC>,
            MinMaxFunction<uint32_t>::combine<FUNC>, MinMaxFunction<uint32_t>::finalize,
            isDistinct);
    case PhysicalTypeID::UINT16:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            resultType, MinMaxFunction<uint16_t>::initialize,
            MinMaxFunction<uint16_t>::updateAll<FUNC>, MinMaxFunction<uint16_t>::updatePos<FUNC>,
            MinMaxFunction<uint16_t>::combine<FUNC>, MinMaxFunction<uint16_t>::finalize,
            isDistinct);
    case PhysicalTypeID::UINT8:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            resultType, MinMaxFunction<uint8_t>::initialize,
            MinMaxFunction<uint8_t>::updateAll<FUNC>, MinMaxFunction<uint8_t>::updatePos<FUNC>,
            MinMaxFunction<uint8_t>::combine<FUNC>, MinMaxFunction<uint8_t>::finalize, isDistinct);
    case PhysicalTypeID::DOUBLE:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            resultType, MinMaxFunction<double>::initialize, MinMaxFunction<double>::updateAll<FUNC>,
            MinMaxFunction<double>::updatePos<FUNC>, MinMaxFunction<double>::combine<FUNC>,
            MinMaxFunction<double>::finalize, isDistinct);
    case PhysicalTypeID::FLOAT:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            resultType, MinMaxFunction<float>::initialize, MinMaxFunction<float>::updateAll<FUNC>,
            MinMaxFunction<float>::updatePos<FUNC>, MinMaxFunction<float>::combine<FUNC>,
            MinMaxFunction<float>::finalize, isDistinct);
    case PhysicalTypeID::INTERVAL:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            resultType, MinMaxFunction<interval_t>::initialize,
            MinMaxFunction<interval_t>::updateAll<FUNC>,
            MinMaxFunction<interval_t>::updatePos<FUNC>, MinMaxFunction<interval_t>::combine<FUNC>,
            MinMaxFunction<interval_t>::finalize, isDistinct);
    case PhysicalTypeID::STRING:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            resultType, MinMaxFunction<ku_string_t>::initialize,
            MinMaxFunction<ku_string_t>::updateAll<FUNC>,
            MinMaxFunction<ku_string_t>::updatePos<FUNC>,
            MinMaxFunction<ku_string_t>::combine<FUNC>, MinMaxFunction<ku_string_t>::finalize,
            isDistinct);
    case PhysicalTypeID::INTERNAL_ID:
        return std::make_unique<AggregateFunction>(std::move(name), std::move(inputTypes),
            resultType, MinMaxFunction<internalID_t>::initialize,
            MinMaxFunction<internalID_t>::updateAll<FUNC>,
            MinMaxFunction<internalID_t>::updatePos<FUNC>,
            MinMaxFunction<internalID_t>::combine<FUNC>, MinMaxFunction<internalID_t>::finalize,
            isDistinct);
    default:
        KU_UNREACHABLE;
    }
}

function_set AggregateSumFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        appendSumFuncs(name, typeID, result);
    }
    return result;
}

function_set AggregateAvgFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        appendAvgFuncs(name, typeID, result);
    }
    return result;
}

function_set AggregateMinFunction::getFunctionSet() {
    function_set result;
    for (auto& type : LogicalTypeUtils::getAllValidComparableLogicalTypes()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            result.push_back(AggregateFunctionUtil::getMinFunc(type, isDistinct));
        }
    }
    return result;
}

function_set AggregateMaxFunction::getFunctionSet() {
    function_set result;
    for (auto& type : LogicalTypeUtils::getAllValidComparableLogicalTypes()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            result.push_back(AggregateFunctionUtil::getMaxFunc(type, isDistinct));
        }
    }
    return result;
}

} // namespace function
} // namespace kuzu
