#include "function/aggregate/aggregate_function.h"

#include "function/aggregate/avg.h"
#include "function/aggregate/collect.h"
#include "function/aggregate/count.h"
#include "function/aggregate/count_star.h"
#include "function/aggregate/min_max.h"
#include "function/aggregate/sum.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace function {

std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getCountStarFunction() {
    return std::make_unique<AggregateFunction>(CountStarFunction::initialize,
        CountStarFunction::updateAll, CountStarFunction::updatePos, CountStarFunction::combine,
        CountStarFunction::finalize, DataType(ANY) /* dummy input data type */);
}

std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getCountFunction(
    const DataType& inputType, bool isDistinct) {
    return std::make_unique<AggregateFunction>(CountFunction::initialize, CountFunction::updateAll,
        CountFunction::updatePos, CountFunction::combine, CountFunction::finalize, inputType,
        isDistinct);
}

std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getAvgFunction(
    const DataType& inputType, bool isDistinct) {
    switch (inputType.typeID) {
    case INT64:
        return std::make_unique<AggregateFunction>(AvgFunction<int64_t>::initialize,
            AvgFunction<int64_t>::updateAll, AvgFunction<int64_t>::updatePos,
            AvgFunction<int64_t>::combine, AvgFunction<int64_t>::finalize, inputType, isDistinct);
    case INT32:
        return std::make_unique<AggregateFunction>(AvgFunction<int32_t>::initialize,
            AvgFunction<int32_t>::updateAll, AvgFunction<int32_t>::updatePos,
            AvgFunction<int32_t>::combine, AvgFunction<int32_t>::finalize, inputType, isDistinct);
    case INT16:
        return std::make_unique<AggregateFunction>(AvgFunction<int16_t>::initialize,
            AvgFunction<int16_t>::updateAll, AvgFunction<int16_t>::updatePos,
            AvgFunction<int16_t>::combine, AvgFunction<int16_t>::finalize, inputType, isDistinct);
    case DOUBLE:
        return std::make_unique<AggregateFunction>(AvgFunction<double_t>::initialize,
            AvgFunction<double_t>::updateAll, AvgFunction<double_t>::updatePos,
            AvgFunction<double_t>::combine, AvgFunction<double_t>::finalize, inputType, isDistinct);
    case FLOAT:
        return std::make_unique<AggregateFunction>(AvgFunction<float_t>::initialize,
            AvgFunction<float_t>::updateAll, AvgFunction<float_t>::updatePos,
            AvgFunction<float_t>::combine, AvgFunction<float_t>::finalize, inputType, isDistinct);
    default:
        throw RuntimeException("Unsupported input data type " + Types::dataTypeToString(inputType) +
                               " for AggregateFunctionUtil::getAvgFunction.");
    }
}

std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getSumFunction(
    const DataType& inputType, bool isDistinct) {
    switch (inputType.typeID) {
    case INT64:
        return std::make_unique<AggregateFunction>(SumFunction<int64_t>::initialize,
            SumFunction<int64_t>::updateAll, SumFunction<int64_t>::updatePos,
            SumFunction<int64_t>::combine, SumFunction<int64_t>::finalize, inputType, isDistinct);
    case INT32:
        return std::make_unique<AggregateFunction>(SumFunction<int32_t>::initialize,
            SumFunction<int32_t>::updateAll, SumFunction<int32_t>::updatePos,
            SumFunction<int32_t>::combine, SumFunction<int32_t>::finalize, inputType, isDistinct);
    case INT16:
        return std::make_unique<AggregateFunction>(SumFunction<int16_t>::initialize,
            SumFunction<int16_t>::updateAll, SumFunction<int16_t>::updatePos,
            SumFunction<int16_t>::combine, SumFunction<int16_t>::finalize, inputType, isDistinct);
    case DOUBLE:
        return std::make_unique<AggregateFunction>(SumFunction<double_t>::initialize,
            SumFunction<double_t>::updateAll, SumFunction<double_t>::updatePos,
            SumFunction<double_t>::combine, SumFunction<double_t>::finalize, inputType, isDistinct);
    case FLOAT:
        return std::make_unique<AggregateFunction>(SumFunction<float_t>::initialize,
            SumFunction<float_t>::updateAll, SumFunction<float_t>::updatePos,
            SumFunction<float_t>::combine, SumFunction<float_t>::finalize, inputType, isDistinct);
    default:
        throw RuntimeException("Unsupported input data type " + Types::dataTypeToString(inputType) +
                               " for AggregateFunctionUtil::getSumFunction.");
    }
}

std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getMinFunction(
    const DataType& inputType, bool isDistinct) {
    return getMinMaxFunction<operation::LessThan>(inputType, isDistinct);
}

std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getMaxFunction(
    const DataType& inputType, bool isDistinct) {
    return getMinMaxFunction<operation::GreaterThan>(inputType, isDistinct);
}

std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getCollectFunction(
    const common::DataType& inputType, bool isDistinct) {
    return std::make_unique<AggregateFunction>(CollectFunction::initialize,
        CollectFunction::updateAll, CollectFunction::updatePos, CollectFunction::combine,
        CollectFunction::finalize, inputType, isDistinct);
}

template<typename FUNC>
std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getMinMaxFunction(
    const DataType& inputType, bool isDistinct) {
    switch (inputType.typeID) {
    case BOOL:
        return std::make_unique<AggregateFunction>(MinMaxFunction<bool>::initialize,
            MinMaxFunction<bool>::updateAll<FUNC>, MinMaxFunction<bool>::updatePos<FUNC>,
            MinMaxFunction<bool>::combine<FUNC>, MinMaxFunction<bool>::finalize, inputType,
            isDistinct);
    case INT64:
        return std::make_unique<AggregateFunction>(MinMaxFunction<int64_t>::initialize,
            MinMaxFunction<int64_t>::updateAll<FUNC>, MinMaxFunction<int64_t>::updatePos<FUNC>,
            MinMaxFunction<int64_t>::combine<FUNC>, MinMaxFunction<int64_t>::finalize, inputType,
            isDistinct);
    case INT32:
        return std::make_unique<AggregateFunction>(MinMaxFunction<int32_t>::initialize,
            MinMaxFunction<int32_t>::updateAll<FUNC>, MinMaxFunction<int32_t>::updatePos<FUNC>,
            MinMaxFunction<int32_t>::combine<FUNC>, MinMaxFunction<int32_t>::finalize, inputType,
            isDistinct);
    case INT16:
        return std::make_unique<AggregateFunction>(MinMaxFunction<int16_t>::initialize,
            MinMaxFunction<int16_t>::updateAll<FUNC>, MinMaxFunction<int16_t>::updatePos<FUNC>,
            MinMaxFunction<int16_t>::combine<FUNC>, MinMaxFunction<int16_t>::finalize, inputType,
            isDistinct);
    case DOUBLE:
        return std::make_unique<AggregateFunction>(MinMaxFunction<double_t>::initialize,
            MinMaxFunction<double_t>::updateAll<FUNC>, MinMaxFunction<double_t>::updatePos<FUNC>,
            MinMaxFunction<double_t>::combine<FUNC>, MinMaxFunction<double_t>::finalize, inputType,
            isDistinct);
    case FLOAT:
        return std::make_unique<AggregateFunction>(MinMaxFunction<float_t>::initialize,
            MinMaxFunction<float_t>::updateAll<FUNC>, MinMaxFunction<float_t>::updatePos<FUNC>,
            MinMaxFunction<float_t>::combine<FUNC>, MinMaxFunction<float_t>::finalize, inputType,
            isDistinct);
    case DATE:
        return std::make_unique<AggregateFunction>(MinMaxFunction<date_t>::initialize,
            MinMaxFunction<date_t>::updateAll<FUNC>, MinMaxFunction<date_t>::updatePos<FUNC>,
            MinMaxFunction<date_t>::combine<FUNC>, MinMaxFunction<date_t>::finalize, inputType,
            isDistinct);
    case TIMESTAMP:
        return std::make_unique<AggregateFunction>(MinMaxFunction<timestamp_t>::initialize,
            MinMaxFunction<timestamp_t>::updateAll<FUNC>,
            MinMaxFunction<timestamp_t>::updatePos<FUNC>,
            MinMaxFunction<timestamp_t>::combine<FUNC>, MinMaxFunction<timestamp_t>::finalize,
            inputType, isDistinct);
    case INTERVAL:
        return std::make_unique<AggregateFunction>(MinMaxFunction<interval_t>::initialize,
            MinMaxFunction<interval_t>::updateAll<FUNC>,
            MinMaxFunction<interval_t>::updatePos<FUNC>, MinMaxFunction<interval_t>::combine<FUNC>,
            MinMaxFunction<interval_t>::finalize, inputType, isDistinct);
    case STRING:
        return std::make_unique<AggregateFunction>(MinMaxFunction<ku_string_t>::initialize,
            MinMaxFunction<ku_string_t>::updateAll<FUNC>,
            MinMaxFunction<ku_string_t>::updatePos<FUNC>,
            MinMaxFunction<ku_string_t>::combine<FUNC>, MinMaxFunction<ku_string_t>::finalize,
            inputType, isDistinct);
    case INTERNAL_ID:
        return std::make_unique<AggregateFunction>(MinMaxFunction<nodeID_t>::initialize,
            MinMaxFunction<nodeID_t>::updateAll<FUNC>, MinMaxFunction<nodeID_t>::updatePos<FUNC>,
            MinMaxFunction<nodeID_t>::combine<FUNC>, MinMaxFunction<nodeID_t>::finalize, inputType,
            isDistinct);
    default:
        throw RuntimeException("Unsupported input data type " + Types::dataTypeToString(inputType) +
                               " for AggregateFunctionUtil::getMinMaxFunction.");
    }
}

} // namespace function
} // namespace kuzu
