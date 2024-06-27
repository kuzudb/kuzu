#include "function/lambda/lambda_function_bind_data.h"
#include "function/lambda/lambda_function_executor.h"
#include "function/list/functions/list_to_string_function.h"
#include "function/list/vector_list_functions.h"

namespace kuzu {
namespace function {

using namespace common;

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* /*function*/) {
    std::vector<LogicalType> paramTypes;
    paramTypes.push_back(arguments[0]->getDataType().copy());
    return std::make_unique<LambdaFunctionBindData>(std::move(paramTypes),
        common::LogicalType::LIST(arguments[1]->getDataType().copy()), arguments[1]);
}

function_set ListTransformFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::LIST,
        LambdaFunctionExecutor::execute<list_entry_t, list_entry_t>);
    function->bindFunc = bindFunc;
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
