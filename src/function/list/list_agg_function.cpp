#include "common/exception/binder.h"
#include "function/list/functions/list_product_function.h"
#include "function/list/functions/list_sum_function.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

template<typename OPERATION>
static std::unique_ptr<FunctionBindData> bindFuncListAggr(
    const binder::expression_vector& arguments, Function* function) {
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    auto resultType = ListType::getChildType(&arguments[0]->dataType);
    switch (resultType->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int64_t, OPERATION>;
    } break;
    case LogicalTypeID::INT32: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int32_t, OPERATION>;
    } break;
    case LogicalTypeID::INT16: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int16_t, OPERATION>;
    } break;
    case LogicalTypeID::INT8: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int8_t, OPERATION>;
    } break;
    case LogicalTypeID::UINT64: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, uint64_t, OPERATION>;
    } break;
    case LogicalTypeID::UINT32: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, uint32_t, OPERATION>;
    } break;
    case LogicalTypeID::UINT16: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, uint16_t, OPERATION>;
    } break;
    case LogicalTypeID::UINT8: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, uint8_t, OPERATION>;
    } break;
    case LogicalTypeID::INT128: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, int128_t, OPERATION>;
    } break;
    case LogicalTypeID::DOUBLE: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, double, OPERATION>;
    } break;
    case LogicalTypeID::FLOAT: {
        scalarFunction->execFunc =
            ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, float, OPERATION>;
    } break;
    default: {
        throw BinderException(stringFormat("Unsupported inner data type for {}: {}", function->name,
            LogicalTypeUtils::toString(resultType->getLogicalTypeID())));
    }
    }
    return FunctionBindData::getSimpleBindData(arguments, *resultType);
}

function_set ListSumFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::LIST},
            LogicalTypeID::INT64, nullptr, nullptr, bindFuncListAggr<ListSum>));
    return result;
}

function_set ListProductFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::LIST},
            LogicalTypeID::INT64, nullptr, nullptr, bindFuncListAggr<ListProduct>));
    return result;
}

} // namespace function
} // namespace kuzu
