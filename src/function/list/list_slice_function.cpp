#include "function/list/functions/list_slice_function.h"

#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* function) {
    KU_ASSERT(arguments.size() == 3);
    std::vector<LogicalType> paramTypes;
    paramTypes.push_back(arguments[0]->getDataType());
    paramTypes.push_back(LogicalType(function->parameterTypeIDs[1]));
    paramTypes.push_back(LogicalType(function->parameterTypeIDs[2]));
    return std::make_unique<FunctionBindData>(std::move(paramTypes),
        arguments[0]->getDataType().copy());
}

function_set ListSliceFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::INT64, LogicalTypeID::INT64},
        LogicalTypeID::LIST,
        ScalarFunction::TernaryExecListStructFunction<list_entry_t, int64_t, int64_t, list_entry_t,
            ListSlice>,
        nullptr, bindFunc));
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64,
            LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        ScalarFunction::TernaryExecListStructFunction<ku_string_t, int64_t, int64_t, ku_string_t,
            ListSlice>,
        nullptr, bindFunc));
    return result;
}

} // namespace function
} // namespace kuzu
