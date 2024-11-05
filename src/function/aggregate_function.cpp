#include "function/aggregate_function.h"

#include "common/type_utils.h"
#include "function/aggregate/avg.h"
#include "function/aggregate/sum.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace function {

AggregateFunction::AggregateFunction(const AggregateFunction& other)
    : ScalarOrAggregateFunction{other.name, other.parameterTypeIDs, other.returnTypeID,
          other.bindFunc} {
    isDistinct = other.isDistinct;
    initializeFunc = other.initializeFunc;
    updateAllFunc = other.updateAllFunc;
    updatePosFunc = other.updatePosFunc;
    combineFunc = other.combineFunc;
    finalizeFunc = other.finalizeFunc;
    paramRewriteFunc = other.paramRewriteFunc;
    initialNullAggregateState = createInitialNullAggregateState();
}

template void AggregateFunctionUtils::appendSumOrAvgFuncs<AvgFunction>(std::string name,
    common::LogicalTypeID inputType, function_set& result);
template void AggregateFunctionUtils::appendSumOrAvgFuncs<SumFunction>(std::string name,
    common::LogicalTypeID inputType, function_set& result);

template<template<typename, typename> class FunctionType>
void AggregateFunctionUtils::appendSumOrAvgFuncs(std::string name, common::LogicalTypeID inputType,
    function_set& result) {
    std::unique_ptr<AggregateFunction> aggFunc;
    for (auto isDistinct : std::vector<bool>{true, false}) {
        TypeUtils::visit(
            LogicalType{inputType},
            [&]<IntegerTypes T>(T) {
                LogicalTypeID resultType = LogicalTypeID::INT128;
                // For avg aggregate functions, the result type is always double.
                if constexpr (std::is_same_v<FunctionType<T, int128_t>, AvgFunction<T, int128_t>>) {
                    resultType = LogicalTypeID::DOUBLE;
                }
                aggFunc = AggregateFunctionUtils::getAggFunc<FunctionType<T, int128_t>>(name,
                    inputType, resultType, isDistinct);
            },
            [&]<FloatingPointTypes T>(T) {
                aggFunc = AggregateFunctionUtils::getAggFunc<FunctionType<T, double>>(name,
                    inputType, LogicalTypeID::DOUBLE, isDistinct);
            },
            [](auto) { KU_UNREACHABLE; });
        result.push_back(std::move(aggFunc));
    }
}

} // namespace function
} // namespace kuzu
