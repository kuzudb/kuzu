#include "src/function/include/aggregate/aggregate_function.h"

#include "src/function/include/aggregate/avg.h"
#include "src/function/include/aggregate/count.h"
#include "src/function/include/aggregate/count_star.h"
#include "src/function/include/aggregate/min_max.h"
#include "src/function/include/aggregate/sum.h"

namespace graphflow {
namespace function {

unique_ptr<AggregateFunction> AggregateFunctionUtil::getAggregateFunction(Expression& expression) {
    assert(isExpressionAggregate(expression.expressionType));
    auto& functionExpression = (FunctionExpression&)expression;
    switch (functionExpression.expressionType) {
    case COUNT_STAR_FUNC: {
        return getCountStarFunction();
    }
    case COUNT_FUNC: {
        return getCountFunction(functionExpression);
    }
    case SUM_FUNC: {
        assert(functionExpression.getNumChildren() == 1);
        return getSumFunction(functionExpression);
    }
    case AVG_FUNC: {
        assert(functionExpression.getNumChildren() == 1);
        return getAvgFunction(functionExpression);
    }
    case MIN_FUNC: {
        assert(functionExpression.getNumChildren() == 1);
        return getMinMaxFunction<true /* IS_MIN */>(functionExpression);
    }
    case MAX_FUNC: {
        assert(functionExpression.getNumChildren() == 1);
        return getMinMaxFunction<false /* IS_MIN */>(functionExpression);
    }
    default:
        throw invalid_argument("Function " +
                               expressionTypeToString(functionExpression.expressionType) +
                               " is not supported.");
    }
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getCountStarFunction() {
    return make_unique<AggregateFunction>(CountStarFunction::initialize, CountStarFunction::update,
        CountStarFunction::combine, CountStarFunction::finalize, INT64 /* dummy input data type */);
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getCountFunction(
    FunctionExpression& functionExpression) {
    return make_unique<AggregateFunction>(CountFunction::initialize, CountFunction::update,
        CountFunction::combine, CountFunction::finalize, functionExpression.getChild(0)->dataType,
        functionExpression.isFunctionDistinct());
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getAvgFunction(
    FunctionExpression& functionExpression) {
    auto inputDataType = functionExpression.getChild(0)->dataType;
    switch (inputDataType) {
    case INT64:
        return make_unique<AggregateFunction>(AvgFunction<int64_t>::initialize,
            AvgFunction<int64_t>::update, AvgFunction<int64_t>::combine,
            AvgFunction<int64_t>::finalize, inputDataType, functionExpression.isFunctionDistinct());
    case DOUBLE:
        return make_unique<AggregateFunction>(AvgFunction<double_t>::initialize,
            AvgFunction<double_t>::update, AvgFunction<double_t>::combine,
            AvgFunction<double_t>::finalize, inputDataType,
            functionExpression.isFunctionDistinct());
    case UNSTRUCTURED:
        return make_unique<AggregateFunction>(AvgFunction<Value>::initialize,
            AvgFunction<Value>::update, AvgFunction<Value>::combine, AvgFunction<Value>::finalize,
            inputDataType, functionExpression.isFunctionDistinct());
    default:
        throw invalid_argument(
            "Data type " + DataTypeNames[inputDataType] + " not supported for AVG.");
    }
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getSumFunction(
    FunctionExpression& functionExpression) {
    auto inputDataType = functionExpression.getChild(0)->dataType;
    switch (inputDataType) {
    case INT64:
        return make_unique<AggregateFunction>(SumFunction<int64_t>::initialize,
            SumFunction<int64_t>::update, SumFunction<int64_t>::combine,
            SumFunction<int64_t>::finalize, inputDataType, functionExpression.isFunctionDistinct());
    case DOUBLE:
        return make_unique<AggregateFunction>(SumFunction<double_t>::initialize,
            SumFunction<double_t>::update, SumFunction<double_t>::combine,
            SumFunction<double_t>::finalize, inputDataType,
            functionExpression.isFunctionDistinct());
    case UNSTRUCTURED:
        return make_unique<AggregateFunction>(SumFunction<Value>::initialize,
            SumFunction<Value>::update, SumFunction<Value>::combine, SumFunction<Value>::finalize,
            inputDataType, functionExpression.isFunctionDistinct());
    default:
        throw invalid_argument(
            "Data type " + DataTypeNames[inputDataType] + " not supported for SUM.");
    }
}

template<bool IS_MIN>
unique_ptr<AggregateFunction> AggregateFunctionUtil::getMinMaxFunction(
    FunctionExpression& functionExpression) {
    auto inputDataType = functionExpression.getChild(0)->dataType;
    if constexpr (IS_MIN) {
        switch (inputDataType) {
        case BOOL:
            return make_unique<AggregateFunction>(MinMaxFunction<bool>::initialize,
                MinMaxFunction<bool>::update<LessThan>, MinMaxFunction<bool>::combine<LessThan>,
                MinMaxFunction<bool>::finalize, inputDataType,
                functionExpression.isFunctionDistinct());
        case INT64:
            return make_unique<AggregateFunction>(MinMaxFunction<int64_t>::initialize,
                MinMaxFunction<int64_t>::update<LessThan>,
                MinMaxFunction<int64_t>::combine<LessThan>, MinMaxFunction<int64_t>::finalize,
                inputDataType, functionExpression.isFunctionDistinct());
        case DOUBLE:
            return make_unique<AggregateFunction>(MinMaxFunction<double_t>::initialize,
                MinMaxFunction<double_t>::update<LessThan>,
                MinMaxFunction<double_t>::combine<LessThan>, MinMaxFunction<double_t>::finalize,
                inputDataType, functionExpression.isFunctionDistinct());
        case DATE:
            return make_unique<AggregateFunction>(MinMaxFunction<date_t>::initialize,
                MinMaxFunction<date_t>::update<LessThan>, MinMaxFunction<date_t>::combine<LessThan>,
                MinMaxFunction<date_t>::finalize, inputDataType,
                functionExpression.isFunctionDistinct());
        case STRING:
            return make_unique<AggregateFunction>(MinMaxFunction<gf_string_t>::initialize,
                MinMaxFunction<gf_string_t>::update<LessThan>,
                MinMaxFunction<gf_string_t>::combine<LessThan>,
                MinMaxFunction<gf_string_t>::finalize, inputDataType,
                functionExpression.isFunctionDistinct());
        case NODE:
            return make_unique<AggregateFunction>(MinMaxFunction<nodeID_t>::initialize,
                MinMaxFunction<nodeID_t>::update<LessThan>,
                MinMaxFunction<nodeID_t>::combine<LessThan>, MinMaxFunction<nodeID_t>::finalize,
                inputDataType, functionExpression.isFunctionDistinct());
        case UNSTRUCTURED:
            return make_unique<AggregateFunction>(MinMaxFunction<Value>::initialize,
                MinMaxFunction<Value>::update<LessThan>, MinMaxFunction<Value>::combine<LessThan>,
                MinMaxFunction<Value>::finalize, inputDataType,
                functionExpression.isFunctionDistinct());
        default:
            throw invalid_argument(
                "Data type " + DataTypeNames[inputDataType] + " not supported for MIN.");
        }
    } else {
        switch (inputDataType) {
        case BOOL:
            return make_unique<AggregateFunction>(MinMaxFunction<bool>::initialize,
                MinMaxFunction<bool>::update<GreaterThan>,
                MinMaxFunction<bool>::combine<GreaterThan>, MinMaxFunction<bool>::finalize,
                inputDataType, functionExpression.isFunctionDistinct());
        case INT64:
            return make_unique<AggregateFunction>(MinMaxFunction<int64_t>::initialize,
                MinMaxFunction<int64_t>::update<GreaterThan>,
                MinMaxFunction<int64_t>::combine<GreaterThan>, MinMaxFunction<int64_t>::finalize,
                inputDataType, functionExpression.isFunctionDistinct());
        case DOUBLE:
            return make_unique<AggregateFunction>(MinMaxFunction<double_t>::initialize,
                MinMaxFunction<double_t>::update<GreaterThan>,
                MinMaxFunction<double_t>::combine<GreaterThan>, MinMaxFunction<double_t>::finalize,
                inputDataType, functionExpression.isFunctionDistinct());
        case DATE:
            return make_unique<AggregateFunction>(MinMaxFunction<date_t>::initialize,
                MinMaxFunction<date_t>::update<GreaterThan>,
                MinMaxFunction<date_t>::combine<GreaterThan>, MinMaxFunction<date_t>::finalize,
                inputDataType, functionExpression.isFunctionDistinct());
        case STRING:
            return make_unique<AggregateFunction>(MinMaxFunction<gf_string_t>::initialize,
                MinMaxFunction<gf_string_t>::update<GreaterThan>,
                MinMaxFunction<gf_string_t>::combine<GreaterThan>,
                MinMaxFunction<gf_string_t>::finalize, inputDataType,
                functionExpression.isFunctionDistinct());
        case NODE:
            return make_unique<AggregateFunction>(MinMaxFunction<nodeID_t>::initialize,
                MinMaxFunction<nodeID_t>::update<GreaterThan>,
                MinMaxFunction<nodeID_t>::combine<GreaterThan>, MinMaxFunction<nodeID_t>::finalize,
                inputDataType, functionExpression.isFunctionDistinct());
        case UNSTRUCTURED:
            return make_unique<AggregateFunction>(MinMaxFunction<Value>::initialize,
                MinMaxFunction<Value>::update<GreaterThan>,
                MinMaxFunction<Value>::combine<GreaterThan>, MinMaxFunction<Value>::finalize,
                inputDataType, functionExpression.isFunctionDistinct());
        default:
            throw invalid_argument(
                "Data type " + DataTypeNames[inputDataType] + " not supported for MAX.");
        }
    }
}

} // namespace function
} // namespace graphflow
