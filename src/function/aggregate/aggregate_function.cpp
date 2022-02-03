#include "src/function/include/aggregate/aggregate_function.h"

#include "src/function/include/aggregate/avg.h"
#include "src/function/include/aggregate/count.h"
#include "src/function/include/aggregate/count_star.h"
#include "src/function/include/aggregate/min_max.h"
#include "src/function/include/aggregate/sum.h"

namespace graphflow {
namespace function {

unique_ptr<AggregateFunction> AggregateFunctionUtil::getAggregateFunction(
    ExpressionType expressionType, DataType dataType) {
    switch (expressionType) {
    case COUNT_FUNC:
        return getCountFunction();
    case SUM_FUNC:
        return getSumFunction(dataType);
    case AVG_FUNC:
        return getAvgFunction(dataType);
    case MIN_FUNC:
        return getMinMaxFunction<true /* IS_MIN */>(dataType);
    case MAX_FUNC:
        return getMinMaxFunction<false /* IS_MIN */>(dataType);
    default:
        throw invalid_argument("Function not supported for now.");
    }
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getCountStarFunction() {
    return make_unique<AggregateFunction>(CountStarFunction::initialize, CountStarFunction::update,
        CountStarFunction::combine, CountStarFunction::finalize);
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getCountFunction() {
    return make_unique<AggregateFunction>(CountFunction::initialize, CountFunction::update,
        CountFunction::combine, CountFunction::finalize);
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getAvgFunction(DataType dataType) {
    switch (dataType) {
    case INT64:
        return make_unique<AggregateFunction>(AvgFunction<int64_t>::initialize,
            AvgFunction<int64_t>::update, AvgFunction<int64_t>::combine,
            AvgFunction<int64_t>::finalize);
    case DOUBLE:
        return make_unique<AggregateFunction>(AvgFunction<double_t>::initialize,
            AvgFunction<double_t>::update, AvgFunction<double_t>::combine,
            AvgFunction<double_t>::finalize);
    case UNSTRUCTURED:
        return make_unique<AggregateFunction>(AvgFunction<Value>::initialize,
            AvgFunction<Value>::update, AvgFunction<Value>::combine, AvgFunction<Value>::finalize);
    default:
        throw invalid_argument("Data type " + DataTypeNames[dataType] + " not supported for AVG.");
    }
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getSumFunction(DataType dataType) {
    switch (dataType) {
    case INT64:
        return make_unique<AggregateFunction>(SumFunction<int64_t>::initialize,
            SumFunction<int64_t>::update, SumFunction<int64_t>::combine,
            SumFunction<int64_t>::finalize);
    case DOUBLE:
        return make_unique<AggregateFunction>(SumFunction<double_t>::initialize,
            SumFunction<double_t>::update, SumFunction<double_t>::combine,
            SumFunction<double_t>::finalize);
    case UNSTRUCTURED:
        return make_unique<AggregateFunction>(SumFunction<Value>::initialize,
            SumFunction<Value>::update, SumFunction<Value>::combine, SumFunction<Value>::finalize);
    default:
        throw invalid_argument("Data type " + DataTypeNames[dataType] + " not supported for SUM.");
    }
}

template<bool IS_MIN>
unique_ptr<AggregateFunction> AggregateFunctionUtil::getMinMaxFunction(DataType dataType) {
    if constexpr (IS_MIN) {
        switch (dataType) {
        case BOOL:
            return make_unique<AggregateFunction>(MinMaxFunction<bool>::initialize,
                MinMaxFunction<bool>::update<LessThan>, MinMaxFunction<bool>::combine<LessThan>,
                MinMaxFunction<bool>::finalize);
        case INT64:
            return make_unique<AggregateFunction>(MinMaxFunction<int64_t>::initialize,
                MinMaxFunction<int64_t>::update<LessThan>,
                MinMaxFunction<int64_t>::combine<LessThan>, MinMaxFunction<int64_t>::finalize);
        case DOUBLE:
            return make_unique<AggregateFunction>(MinMaxFunction<double_t>::initialize,
                MinMaxFunction<double_t>::update<LessThan>,
                MinMaxFunction<double_t>::combine<LessThan>, MinMaxFunction<double_t>::finalize);
        case DATE:
            return make_unique<AggregateFunction>(MinMaxFunction<date_t>::initialize,
                MinMaxFunction<date_t>::update<LessThan>, MinMaxFunction<date_t>::combine<LessThan>,
                MinMaxFunction<date_t>::finalize);
        case STRING:
            return make_unique<AggregateFunction>(MinMaxFunction<gf_string_t>::initialize,
                MinMaxFunction<gf_string_t>::update<LessThan>,
                MinMaxFunction<gf_string_t>::combine<LessThan>,
                MinMaxFunction<gf_string_t>::finalize);
        case NODE:
            return make_unique<AggregateFunction>(MinMaxFunction<nodeID_t>::initialize,
                MinMaxFunction<nodeID_t>::update<LessThan>,
                MinMaxFunction<nodeID_t>::combine<LessThan>, MinMaxFunction<nodeID_t>::finalize);
        case UNSTRUCTURED:
            return make_unique<AggregateFunction>(MinMaxFunction<Value>::initialize,
                MinMaxFunction<Value>::update<LessThan>, MinMaxFunction<Value>::combine<LessThan>,
                MinMaxFunction<Value>::finalize);
        default:
            throw invalid_argument(
                "Data type " + DataTypeNames[dataType] + " not supported for MIN.");
        }
    } else {
        switch (dataType) {
        case BOOL:
            return make_unique<AggregateFunction>(MinMaxFunction<bool>::initialize,
                MinMaxFunction<bool>::update<GreaterThan>,
                MinMaxFunction<bool>::combine<GreaterThan>, MinMaxFunction<bool>::finalize);
        case INT64:
            return make_unique<AggregateFunction>(MinMaxFunction<int64_t>::initialize,
                MinMaxFunction<int64_t>::update<GreaterThan>,
                MinMaxFunction<int64_t>::combine<GreaterThan>, MinMaxFunction<int64_t>::finalize);
        case DOUBLE:
            return make_unique<AggregateFunction>(MinMaxFunction<double_t>::initialize,
                MinMaxFunction<double_t>::update<GreaterThan>,
                MinMaxFunction<double_t>::combine<GreaterThan>, MinMaxFunction<double_t>::finalize);
        case DATE:
            return make_unique<AggregateFunction>(MinMaxFunction<date_t>::initialize,
                MinMaxFunction<date_t>::update<GreaterThan>,
                MinMaxFunction<date_t>::combine<GreaterThan>, MinMaxFunction<date_t>::finalize);
        case STRING:
            return make_unique<AggregateFunction>(MinMaxFunction<gf_string_t>::initialize,
                MinMaxFunction<gf_string_t>::update<GreaterThan>,
                MinMaxFunction<gf_string_t>::combine<GreaterThan>,
                MinMaxFunction<gf_string_t>::finalize);
        case NODE:
            return make_unique<AggregateFunction>(MinMaxFunction<nodeID_t>::initialize,
                MinMaxFunction<nodeID_t>::update<GreaterThan>,
                MinMaxFunction<nodeID_t>::combine<GreaterThan>, MinMaxFunction<nodeID_t>::finalize);
        case UNSTRUCTURED:
            return make_unique<AggregateFunction>(MinMaxFunction<Value>::initialize,
                MinMaxFunction<Value>::update<GreaterThan>,
                MinMaxFunction<Value>::combine<GreaterThan>, MinMaxFunction<Value>::finalize);
        default:
            throw invalid_argument(
                "Data type " + DataTypeNames[dataType] + " not supported for MAX.");
        }
    }
}

} // namespace function
} // namespace graphflow
