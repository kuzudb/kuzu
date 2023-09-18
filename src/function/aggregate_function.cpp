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
        CountStarFunction::finalize, LogicalType() /* dummy input data type */);
}

std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getCountFunction(
    const LogicalType& inputType, bool isDistinct) {
    return std::make_unique<AggregateFunction>(CountFunction::initialize, CountFunction::updateAll,
        CountFunction::updatePos, CountFunction::combine, CountFunction::finalize, inputType,
        isDistinct);
}

std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getAvgFunction(
    const LogicalType& inputType, bool isDistinct) {
    switch (inputType.getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64:
        return std::make_unique<AggregateFunction>(AvgFunction<int64_t>::initialize,
            AvgFunction<int64_t>::updateAll, AvgFunction<int64_t>::updatePos,
            AvgFunction<int64_t>::combine, AvgFunction<int64_t>::finalize, inputType, isDistinct);
    case LogicalTypeID::INT32:
        return std::make_unique<AggregateFunction>(AvgFunction<int32_t>::initialize,
            AvgFunction<int32_t>::updateAll, AvgFunction<int32_t>::updatePos,
            AvgFunction<int32_t>::combine, AvgFunction<int32_t>::finalize, inputType, isDistinct);
    case LogicalTypeID::INT16:
        return std::make_unique<AggregateFunction>(AvgFunction<int16_t>::initialize,
            AvgFunction<int16_t>::updateAll, AvgFunction<int16_t>::updatePos,
            AvgFunction<int16_t>::combine, AvgFunction<int16_t>::finalize, inputType, isDistinct);
    case LogicalTypeID::INT8:
        return std::make_unique<AggregateFunction>(AvgFunction<int8_t>::initialize,
            AvgFunction<int8_t>::updateAll, AvgFunction<int8_t>::updatePos,
            AvgFunction<int8_t>::combine, AvgFunction<int8_t>::finalize, inputType, isDistinct);
    case LogicalTypeID::UINT64:
        return std::make_unique<AggregateFunction>(AvgFunction<uint64_t>::initialize,
            AvgFunction<uint64_t>::updateAll, AvgFunction<uint64_t>::updatePos,
            AvgFunction<uint64_t>::combine, AvgFunction<uint64_t>::finalize, inputType, isDistinct);
    case LogicalTypeID::UINT32:
        return std::make_unique<AggregateFunction>(AvgFunction<uint32_t>::initialize,
            AvgFunction<uint32_t>::updateAll, AvgFunction<uint32_t>::updatePos,
            AvgFunction<uint32_t>::combine, AvgFunction<uint32_t>::finalize, inputType, isDistinct);
    case LogicalTypeID::UINT16:
        return std::make_unique<AggregateFunction>(AvgFunction<uint16_t>::initialize,
            AvgFunction<uint16_t>::updateAll, AvgFunction<uint16_t>::updatePos,
            AvgFunction<uint16_t>::combine, AvgFunction<uint16_t>::finalize, inputType, isDistinct);
    case LogicalTypeID::UINT8:
        return std::make_unique<AggregateFunction>(AvgFunction<uint8_t>::initialize,
            AvgFunction<uint8_t>::updateAll, AvgFunction<uint8_t>::updatePos,
            AvgFunction<uint8_t>::combine, AvgFunction<uint8_t>::finalize, inputType, isDistinct);
    case LogicalTypeID::DOUBLE:
        return std::make_unique<AggregateFunction>(AvgFunction<double_t>::initialize,
            AvgFunction<double_t>::updateAll, AvgFunction<double_t>::updatePos,
            AvgFunction<double_t>::combine, AvgFunction<double_t>::finalize, inputType, isDistinct);
    case LogicalTypeID::FLOAT:
        return std::make_unique<AggregateFunction>(AvgFunction<float_t>::initialize,
            AvgFunction<float_t>::updateAll, AvgFunction<float_t>::updatePos,
            AvgFunction<float_t>::combine, AvgFunction<float_t>::finalize, inputType, isDistinct);
    default:
        throw RuntimeException("Unsupported input data type " +
                               LogicalTypeUtils::dataTypeToString(inputType) +
                               " for AggregateFunctionUtil::getAvgFunction.");
    }
}

std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getSumFunction(
    const LogicalType& inputType, bool isDistinct) {
    switch (inputType.getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64:
        return std::make_unique<AggregateFunction>(SumFunction<int64_t>::initialize,
            SumFunction<int64_t>::updateAll, SumFunction<int64_t>::updatePos,
            SumFunction<int64_t>::combine, SumFunction<int64_t>::finalize, inputType, isDistinct);
    case LogicalTypeID::INT32:
        return std::make_unique<AggregateFunction>(SumFunction<int32_t>::initialize,
            SumFunction<int32_t>::updateAll, SumFunction<int32_t>::updatePos,
            SumFunction<int32_t>::combine, SumFunction<int32_t>::finalize, inputType, isDistinct);
    case LogicalTypeID::INT16:
        return std::make_unique<AggregateFunction>(SumFunction<int16_t>::initialize,
            SumFunction<int16_t>::updateAll, SumFunction<int16_t>::updatePos,
            SumFunction<int16_t>::combine, SumFunction<int16_t>::finalize, inputType, isDistinct);
    case LogicalTypeID::INT8:
        return std::make_unique<AggregateFunction>(SumFunction<int8_t>::initialize,
            SumFunction<int8_t>::updateAll, SumFunction<int8_t>::updatePos,
            SumFunction<int8_t>::combine, SumFunction<int8_t>::finalize, inputType, isDistinct);
    case LogicalTypeID::UINT64:
        return std::make_unique<AggregateFunction>(SumFunction<uint64_t>::initialize,
            SumFunction<uint64_t>::updateAll, SumFunction<uint64_t>::updatePos,
            SumFunction<uint64_t>::combine, SumFunction<uint64_t>::finalize, inputType, isDistinct);
    case LogicalTypeID::UINT32:
        return std::make_unique<AggregateFunction>(SumFunction<uint32_t>::initialize,
            SumFunction<uint32_t>::updateAll, SumFunction<uint32_t>::updatePos,
            SumFunction<uint32_t>::combine, SumFunction<uint32_t>::finalize, inputType, isDistinct);
    case LogicalTypeID::UINT16:
        return std::make_unique<AggregateFunction>(SumFunction<uint16_t>::initialize,
            SumFunction<uint16_t>::updateAll, SumFunction<uint16_t>::updatePos,
            SumFunction<uint16_t>::combine, SumFunction<uint16_t>::finalize, inputType, isDistinct);
    case LogicalTypeID::UINT8:
        return std::make_unique<AggregateFunction>(SumFunction<uint8_t>::initialize,
            SumFunction<uint8_t>::updateAll, SumFunction<uint8_t>::updatePos,
            SumFunction<uint8_t>::combine, SumFunction<uint8_t>::finalize, inputType, isDistinct);
    case LogicalTypeID::DOUBLE:
        return std::make_unique<AggregateFunction>(SumFunction<double_t>::initialize,
            SumFunction<double_t>::updateAll, SumFunction<double_t>::updatePos,
            SumFunction<double_t>::combine, SumFunction<double_t>::finalize, inputType, isDistinct);
    case LogicalTypeID::FLOAT:
        return std::make_unique<AggregateFunction>(SumFunction<float_t>::initialize,
            SumFunction<float_t>::updateAll, SumFunction<float_t>::updatePos,
            SumFunction<float_t>::combine, SumFunction<float_t>::finalize, inputType, isDistinct);
    default:
        throw RuntimeException("Unsupported input data type " +
                               LogicalTypeUtils::dataTypeToString(inputType) +
                               " for AggregateFunctionUtil::getSumFunction.");
    }
}

std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getMinFunction(
    const LogicalType& inputType, bool isDistinct) {
    return getMinMaxFunction<LessThan>(inputType, isDistinct);
}

std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getMaxFunction(
    const LogicalType& inputType, bool isDistinct) {
    return getMinMaxFunction<GreaterThan>(inputType, isDistinct);
}

std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getCollectFunction(
    const LogicalType& inputType, bool isDistinct) {
    return std::make_unique<AggregateFunction>(CollectFunction::initialize,
        CollectFunction::updateAll, CollectFunction::updatePos, CollectFunction::combine,
        CollectFunction::finalize, inputType, isDistinct);
}

template<typename FUNC>
std::unique_ptr<AggregateFunction> AggregateFunctionUtil::getMinMaxFunction(
    const LogicalType& inputType, bool isDistinct) {
    switch (inputType.getPhysicalType()) {
    case PhysicalTypeID::BOOL:
        return std::make_unique<AggregateFunction>(MinMaxFunction<bool>::initialize,
            MinMaxFunction<bool>::updateAll<FUNC>, MinMaxFunction<bool>::updatePos<FUNC>,
            MinMaxFunction<bool>::combine<FUNC>, MinMaxFunction<bool>::finalize, inputType,
            isDistinct);
    case PhysicalTypeID::INT64:
        return std::make_unique<AggregateFunction>(MinMaxFunction<int64_t>::initialize,
            MinMaxFunction<int64_t>::updateAll<FUNC>, MinMaxFunction<int64_t>::updatePos<FUNC>,
            MinMaxFunction<int64_t>::combine<FUNC>, MinMaxFunction<int64_t>::finalize, inputType,
            isDistinct);
    case PhysicalTypeID::INT32:
        return std::make_unique<AggregateFunction>(MinMaxFunction<int32_t>::initialize,
            MinMaxFunction<int32_t>::updateAll<FUNC>, MinMaxFunction<int32_t>::updatePos<FUNC>,
            MinMaxFunction<int32_t>::combine<FUNC>, MinMaxFunction<int32_t>::finalize, inputType,
            isDistinct);
    case PhysicalTypeID::INT16:
        return std::make_unique<AggregateFunction>(MinMaxFunction<int16_t>::initialize,
            MinMaxFunction<int16_t>::updateAll<FUNC>, MinMaxFunction<int16_t>::updatePos<FUNC>,
            MinMaxFunction<int16_t>::combine<FUNC>, MinMaxFunction<int16_t>::finalize, inputType,
            isDistinct);
    case PhysicalTypeID::INT8:
        return std::make_unique<AggregateFunction>(MinMaxFunction<int8_t>::initialize,
            MinMaxFunction<int8_t>::updateAll<FUNC>, MinMaxFunction<int8_t>::updatePos<FUNC>,
            MinMaxFunction<int8_t>::combine<FUNC>, MinMaxFunction<int8_t>::finalize, inputType,
            isDistinct);
    case PhysicalTypeID::DOUBLE:
        return std::make_unique<AggregateFunction>(MinMaxFunction<double_t>::initialize,
            MinMaxFunction<double_t>::updateAll<FUNC>, MinMaxFunction<double_t>::updatePos<FUNC>,
            MinMaxFunction<double_t>::combine<FUNC>, MinMaxFunction<double_t>::finalize, inputType,
            isDistinct);
    case PhysicalTypeID::UINT64:
        return std::make_unique<AggregateFunction>(MinMaxFunction<uint64_t>::initialize,
            MinMaxFunction<uint64_t>::updateAll<FUNC>, MinMaxFunction<uint64_t>::updatePos<FUNC>,
            MinMaxFunction<uint64_t>::combine<FUNC>, MinMaxFunction<uint64_t>::finalize, inputType,
            isDistinct);
    case PhysicalTypeID::UINT32:
        return std::make_unique<AggregateFunction>(MinMaxFunction<uint32_t>::initialize,
            MinMaxFunction<uint32_t>::updateAll<FUNC>, MinMaxFunction<uint32_t>::updatePos<FUNC>,
            MinMaxFunction<uint32_t>::combine<FUNC>, MinMaxFunction<uint32_t>::finalize, inputType,
            isDistinct);
    case PhysicalTypeID::UINT16:
        return std::make_unique<AggregateFunction>(MinMaxFunction<uint16_t>::initialize,
            MinMaxFunction<uint16_t>::updateAll<FUNC>, MinMaxFunction<uint16_t>::updatePos<FUNC>,
            MinMaxFunction<uint16_t>::combine<FUNC>, MinMaxFunction<uint16_t>::finalize, inputType,
            isDistinct);
    case PhysicalTypeID::UINT8:
        return std::make_unique<AggregateFunction>(MinMaxFunction<uint8_t>::initialize,
            MinMaxFunction<uint8_t>::updateAll<FUNC>, MinMaxFunction<uint8_t>::updatePos<FUNC>,
            MinMaxFunction<uint8_t>::combine<FUNC>, MinMaxFunction<uint8_t>::finalize, inputType,
            isDistinct);
    case PhysicalTypeID::FLOAT:
        return std::make_unique<AggregateFunction>(MinMaxFunction<float_t>::initialize,
            MinMaxFunction<float_t>::updateAll<FUNC>, MinMaxFunction<float_t>::updatePos<FUNC>,
            MinMaxFunction<float_t>::combine<FUNC>, MinMaxFunction<float_t>::finalize, inputType,
            isDistinct);
    case PhysicalTypeID::INTERVAL:
        return std::make_unique<AggregateFunction>(MinMaxFunction<interval_t>::initialize,
            MinMaxFunction<interval_t>::updateAll<FUNC>,
            MinMaxFunction<interval_t>::updatePos<FUNC>, MinMaxFunction<interval_t>::combine<FUNC>,
            MinMaxFunction<interval_t>::finalize, inputType, isDistinct);
    case PhysicalTypeID::STRING:
        return std::make_unique<AggregateFunction>(MinMaxFunction<ku_string_t>::initialize,
            MinMaxFunction<ku_string_t>::updateAll<FUNC>,
            MinMaxFunction<ku_string_t>::updatePos<FUNC>,
            MinMaxFunction<ku_string_t>::combine<FUNC>, MinMaxFunction<ku_string_t>::finalize,
            inputType, isDistinct);
    case PhysicalTypeID::INTERNAL_ID:
        return std::make_unique<AggregateFunction>(MinMaxFunction<nodeID_t>::initialize,
            MinMaxFunction<nodeID_t>::updateAll<FUNC>, MinMaxFunction<nodeID_t>::updatePos<FUNC>,
            MinMaxFunction<nodeID_t>::combine<FUNC>, MinMaxFunction<nodeID_t>::finalize, inputType,
            isDistinct);
    default:
        throw RuntimeException("Unsupported input data type " +
                               LogicalTypeUtils::dataTypeToString(inputType) +
                               " for AggregateFunctionUtil::getMinMaxFunction.");
    }
}

} // namespace function
} // namespace kuzu
