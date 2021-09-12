#include "src/expression_evaluator/include/aggregate_expression_evaluator.h"

#include "src/function/include/aggregation/avg.h"
#include "src/function/include/aggregation/count.h"
#include "src/function/include/aggregation/min_max.h"
#include "src/function/include/aggregation/sum.h"

namespace graphflow {
namespace evaluator {

unique_ptr<AggregationFunction> AggregateExpressionEvaluator::getAggregationFunction(
    ExpressionType expressionType, DataType dataType) {
    switch (expressionType) {
    case COUNT_STAR_FUNC:
        return getCountStarFunction();
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

unique_ptr<AggregationFunction> AggregateExpressionEvaluator::getCountStarFunction() {
    return make_unique<AggregationFunction>(CountFunction<true /* IS_COUNT_STAR */>::initialize,
        CountFunction<true /* IS_COUNT_STAR */>::update,
        CountFunction<true /* IS_COUNT_STAR */>::combine,
        CountFunction<true /* IS_COUNT_STAR */>::finalize);
}

unique_ptr<AggregationFunction> AggregateExpressionEvaluator::getCountFunction() {
    return make_unique<AggregationFunction>(CountFunction<false /* IS_COUNT_STAR */>::initialize,
        CountFunction<false /* IS_COUNT_STAR */>::update,
        CountFunction<false /* IS_COUNT_STAR */>::combine,
        CountFunction<false /* IS_COUNT_STAR */>::finalize);
}

unique_ptr<AggregationFunction> AggregateExpressionEvaluator::getAvgFunction(DataType dataType) {
    switch (dataType) {
    case INT64:
        return make_unique<AggregationFunction>(AvgFunction<uint64_t>::initialize,
            AvgFunction<uint64_t>::update, AvgFunction<uint64_t>::combine,
            AvgFunction<uint64_t>::finalize);
    case DOUBLE:
        return make_unique<AggregationFunction>(AvgFunction<double_t>::initialize,
            AvgFunction<double_t>::update, AvgFunction<double_t>::combine,
            AvgFunction<double_t>::finalize);
    default:
        throw invalid_argument("Data type " + DataTypeNames[dataType] + " not supported for AVG.");
    }
}

unique_ptr<AggregationFunction> AggregateExpressionEvaluator::getSumFunction(DataType dataType) {
    switch (dataType) {
    case INT64:
        return make_unique<AggregationFunction>(SumFunction<uint64_t>::initialize,
            SumFunction<uint64_t>::update, SumFunction<uint64_t>::combine,
            SumFunction<uint64_t>::finalize);
    case DOUBLE:
        return make_unique<AggregationFunction>(SumFunction<double_t>::initialize,
            SumFunction<double_t>::update, SumFunction<double_t>::combine,
            SumFunction<double_t>::finalize);
    case UNSTRUCTURED:
        return make_unique<AggregationFunction>(SumFunction<Value>::initialize,
            SumFunction<Value>::update, SumFunction<Value>::combine, SumFunction<Value>::finalize);
    default:
        throw invalid_argument("Data type " + DataTypeNames[dataType] + " not supported for SUM.");
    }
}

template<bool IS_MIN>
unique_ptr<AggregationFunction> AggregateExpressionEvaluator::getMinMaxFunction(DataType dataType) {
    if constexpr (IS_MIN) {
        switch (dataType) {
        case INT64:
            return make_unique<AggregationFunction>(MinMaxFunction<int64_t>::initialize,
                MinMaxFunction<int64_t>::update<LessThan>,
                MinMaxFunction<int64_t>::combine<LessThan>, MinMaxFunction<int64_t>::finalize);
        case DOUBLE:
            return make_unique<AggregationFunction>(MinMaxFunction<double_t>::initialize,
                MinMaxFunction<double_t>::update<LessThan>,
                MinMaxFunction<double_t>::combine<LessThan>, MinMaxFunction<double_t>::finalize);
        case DATE:
            return make_unique<AggregationFunction>(MinMaxFunction<date_t>::initialize,
                MinMaxFunction<date_t>::update<LessThan>, MinMaxFunction<date_t>::combine<LessThan>,
                MinMaxFunction<date_t>::finalize);
        case STRING:
            return make_unique<AggregationFunction>(MinMaxFunction<gf_string_t>::initialize,
                MinMaxFunction<gf_string_t>::update<LessThan>,
                MinMaxFunction<gf_string_t>::combine<LessThan>,
                MinMaxFunction<gf_string_t>::finalize);
        case NODE:
            return make_unique<AggregationFunction>(MinMaxFunction<nodeID_t>::initialize,
                MinMaxFunction<nodeID_t>::update<LessThan>,
                MinMaxFunction<nodeID_t>::combine<LessThan>, MinMaxFunction<nodeID_t>::finalize);
        case UNSTRUCTURED:
            return make_unique<AggregationFunction>(MinMaxFunction<Value>::initialize,
                MinMaxFunction<Value>::update<LessThan>, MinMaxFunction<Value>::combine<LessThan>,
                MinMaxFunction<Value>::finalize);
        default:
            throw invalid_argument(
                "Data type " + DataTypeNames[dataType] + " not supported for MIN.");
        }
    } else {
        switch (dataType) {
        case INT64:
            return make_unique<AggregationFunction>(MinMaxFunction<int64_t>::initialize,
                MinMaxFunction<int64_t>::update<GreaterThan>,
                MinMaxFunction<int64_t>::combine<GreaterThan>, MinMaxFunction<int64_t>::finalize);
        case DOUBLE:
            return make_unique<AggregationFunction>(MinMaxFunction<double_t>::initialize,
                MinMaxFunction<double_t>::update<GreaterThan>,
                MinMaxFunction<double_t>::combine<GreaterThan>, MinMaxFunction<double_t>::finalize);
        case DATE:
            return make_unique<AggregationFunction>(MinMaxFunction<date_t>::initialize,
                MinMaxFunction<date_t>::update<GreaterThan>,
                MinMaxFunction<date_t>::combine<GreaterThan>, MinMaxFunction<date_t>::finalize);
        case STRING:
            return make_unique<AggregationFunction>(MinMaxFunction<gf_string_t>::initialize,
                MinMaxFunction<gf_string_t>::update<GreaterThan>,
                MinMaxFunction<gf_string_t>::combine<GreaterThan>,
                MinMaxFunction<gf_string_t>::finalize);
        case NODE:
            return make_unique<AggregationFunction>(MinMaxFunction<nodeID_t>::initialize,
                MinMaxFunction<nodeID_t>::update<GreaterThan>,
                MinMaxFunction<nodeID_t>::combine<GreaterThan>, MinMaxFunction<nodeID_t>::finalize);
        case UNSTRUCTURED:
            return make_unique<AggregationFunction>(MinMaxFunction<Value>::initialize,
                MinMaxFunction<Value>::update<GreaterThan>,
                MinMaxFunction<Value>::combine<GreaterThan>, MinMaxFunction<Value>::finalize);
        default:
            throw invalid_argument(
                "Data type " + DataTypeNames[dataType] + " not supported for MAX.");
        }
    }
}

} // namespace evaluator
} // namespace graphflow
