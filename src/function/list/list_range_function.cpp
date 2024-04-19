#include "function/list/functions/list_range_function.h"

#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

template<typename OPERATION, typename RESULT_TYPE>
static scalar_func_exec_t getBinaryListExecFuncSwitchAll(const LogicalType& type) {
    scalar_func_exec_t execFunc;
    switch (type.getPhysicalType()) {
    case PhysicalTypeID::INT64: {
        execFunc =
            ScalarFunction::BinaryExecListStructFunction<int64_t, int64_t, RESULT_TYPE, OPERATION>;
    } break;
    case PhysicalTypeID::INT32: {
        execFunc =
            ScalarFunction::BinaryExecListStructFunction<int32_t, int32_t, RESULT_TYPE, OPERATION>;
    } break;
    case PhysicalTypeID::INT16: {
        execFunc =
            ScalarFunction::BinaryExecListStructFunction<int16_t, int16_t, RESULT_TYPE, OPERATION>;
    } break;
    case PhysicalTypeID::INT8: {
        execFunc =
            ScalarFunction::BinaryExecListStructFunction<int8_t, int8_t, RESULT_TYPE, OPERATION>;
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return execFunc;
}

template<typename OPERATION, typename RESULT_TYPE>
static scalar_func_exec_t getTernaryListExecFuncSwitchAll(const LogicalType& type) {
    scalar_func_exec_t execFunc;
    switch (type.getPhysicalType()) {
    case PhysicalTypeID::INT64: {
        execFunc = ScalarFunction::TernaryExecListStructFunction<int64_t, int64_t, int64_t,
            RESULT_TYPE, OPERATION>;
    } break;
    case PhysicalTypeID::INT32: {
        execFunc = ScalarFunction::TernaryExecListStructFunction<int32_t, int32_t, int32_t,
            RESULT_TYPE, OPERATION>;
    } break;
    case PhysicalTypeID::INT16: {
        execFunc = ScalarFunction::TernaryExecListStructFunction<int16_t, int16_t, int16_t,
            RESULT_TYPE, OPERATION>;
    } break;
    case PhysicalTypeID::INT8: {
        execFunc = ScalarFunction::TernaryExecListStructFunction<int8_t, int8_t, int8_t,
            RESULT_TYPE, OPERATION>;
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return execFunc;
}

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* function) {
    auto type = LogicalType(function->parameterTypeIDs[0]);
    auto resultType = LogicalType::LIST(type.copy());
    auto bindData = std::make_unique<FunctionBindData>(std::move(resultType));
    for (auto& _ : arguments) {
        (void)_;
        bindData->paramTypes.push_back(type);
    }
    return bindData;
}

function_set ListRangeFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getIntegerLogicalTypeIDs()) {
        // start, end
        result.push_back(std::make_unique<ScalarFunction>(name,
            std::vector<LogicalTypeID>{typeID, typeID}, LogicalTypeID::LIST,
            getBinaryListExecFuncSwitchAll<Range, list_entry_t>(LogicalType{typeID}), nullptr,
            bindFunc));
        // start, end, step
        result.push_back(std::make_unique<ScalarFunction>(name,
            std::vector<LogicalTypeID>{typeID, typeID, typeID}, LogicalTypeID::LIST,
            getTernaryListExecFuncSwitchAll<Range, list_entry_t>(LogicalType{typeID}), nullptr,
            bindFunc));
    }
    return result;
}

} // namespace function
} // namespace kuzu
