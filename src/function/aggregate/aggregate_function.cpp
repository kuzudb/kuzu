#include "include/aggregate_function.h"

#include "include/avg.h"
#include "include/count.h"
#include "include/count_star.h"
#include "include/min_max.h"
#include "include/sum.h"

namespace graphflow {
namespace function {

unique_ptr<AggregateFunction> AggregateFunctionUtil::getCountStarFunction() {
    return make_unique<AggregateFunction>(CountStarFunction::initialize, CountStarFunction::update,
        CountStarFunction::combine, CountStarFunction::finalize,
        DataType(ANY) /* dummy input data type */);
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getCountFunction(
    const DataType& inputType, bool isDistinct) {
    return make_unique<AggregateFunction>(CountFunction::initialize, CountFunction::update,
        CountFunction::combine, CountFunction::finalize, inputType, isDistinct);
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getAvgFunction(
    const DataType& inputType, bool isDistinct) {
    switch (inputType.typeID) {
    case INT64:
        return make_unique<AggregateFunction>(AvgFunction<int64_t>::initialize,
            AvgFunction<int64_t>::update, AvgFunction<int64_t>::combine,
            AvgFunction<int64_t>::finalize, inputType, isDistinct);
    case DOUBLE:
        return make_unique<AggregateFunction>(AvgFunction<double_t>::initialize,
            AvgFunction<double_t>::update, AvgFunction<double_t>::combine,
            AvgFunction<double_t>::finalize, inputType, isDistinct);
    case UNSTRUCTURED:
        return make_unique<AggregateFunction>(AvgFunction<Value>::initialize,
            AvgFunction<Value>::update, AvgFunction<Value>::combine, AvgFunction<Value>::finalize,
            inputType, isDistinct);
    default:
        assert(false);
    }
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getSumFunction(
    const DataType& inputType, bool isDistinct) {
    switch (inputType.typeID) {
    case INT64:
        return make_unique<AggregateFunction>(SumFunction<int64_t>::initialize,
            SumFunction<int64_t>::update, SumFunction<int64_t>::combine,
            SumFunction<int64_t>::finalize, inputType, isDistinct);
    case DOUBLE:
        return make_unique<AggregateFunction>(SumFunction<double_t>::initialize,
            SumFunction<double_t>::update, SumFunction<double_t>::combine,
            SumFunction<double_t>::finalize, inputType, isDistinct);
    case UNSTRUCTURED:
        return make_unique<AggregateFunction>(SumFunction<Value>::initialize,
            SumFunction<Value>::update, SumFunction<Value>::combine, SumFunction<Value>::finalize,
            inputType, isDistinct);
    default:
        assert(false);
    }
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getMinFunction(
    const DataType& inputType, bool isDistinct) {
    return getMinMaxFunction<LessThan>(inputType, isDistinct);
}

unique_ptr<AggregateFunction> AggregateFunctionUtil::getMaxFunction(
    const DataType& inputType, bool isDistinct) {
    return getMinMaxFunction<GreaterThan>(inputType, isDistinct);
}

template<typename FUNC>
unique_ptr<AggregateFunction> AggregateFunctionUtil::getMinMaxFunction(
    const DataType& inputType, bool isDistinct) {
    switch (inputType.typeID) {
    case BOOL:
        return make_unique<AggregateFunction>(MinMaxFunction<bool>::initialize,
            MinMaxFunction<bool>::update<FUNC>, MinMaxFunction<bool>::combine<FUNC>,
            MinMaxFunction<bool>::finalize, inputType, isDistinct);
    case INT64:
        return make_unique<AggregateFunction>(MinMaxFunction<int64_t>::initialize,
            MinMaxFunction<int64_t>::update<FUNC>, MinMaxFunction<int64_t>::combine<FUNC>,
            MinMaxFunction<int64_t>::finalize, inputType, isDistinct);
    case DOUBLE:
        return make_unique<AggregateFunction>(MinMaxFunction<double_t>::initialize,
            MinMaxFunction<double_t>::update<FUNC>, MinMaxFunction<double_t>::combine<FUNC>,
            MinMaxFunction<double_t>::finalize, inputType, isDistinct);
    case DATE:
        return make_unique<AggregateFunction>(MinMaxFunction<date_t>::initialize,
            MinMaxFunction<date_t>::update<FUNC>, MinMaxFunction<date_t>::combine<FUNC>,
            MinMaxFunction<date_t>::finalize, inputType, isDistinct);
    case STRING:
        return make_unique<AggregateFunction>(MinMaxFunction<gf_string_t>::initialize,
            MinMaxFunction<gf_string_t>::update<FUNC>, MinMaxFunction<gf_string_t>::combine<FUNC>,
            MinMaxFunction<gf_string_t>::finalize, inputType, isDistinct);
    case NODE:
        return make_unique<AggregateFunction>(MinMaxFunction<nodeID_t>::initialize,
            MinMaxFunction<nodeID_t>::update<FUNC>, MinMaxFunction<nodeID_t>::combine<FUNC>,
            MinMaxFunction<nodeID_t>::finalize, inputType, isDistinct);
    case UNSTRUCTURED:
        return make_unique<AggregateFunction>(MinMaxFunction<Value>::initialize,
            MinMaxFunction<Value>::update<FUNC>, MinMaxFunction<Value>::combine<FUNC>,
            MinMaxFunction<Value>::finalize, inputType, isDistinct);
    default:
        assert(false);
    }
}

} // namespace function
} // namespace graphflow
