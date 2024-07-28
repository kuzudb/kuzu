#include "binder/expression/expression_util.h"
#include "function/array/vector_array_functions.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(ScalarBindFuncInput input) {
    LogicalType combinedType(LogicalTypeID::ANY);
    binder::ExpressionUtil::tryCombineDataType(input.arguments, combinedType);
    if (combinedType.getLogicalTypeID() == LogicalTypeID::ANY) {
        combinedType = LogicalType::STRING();
    }
    auto resultType = LogicalType::ARRAY(combinedType.copy(), input.arguments.size());
    auto bindData = std::make_unique<FunctionBindData>(std::move(resultType));
    for (auto& _ : input.arguments) {
        (void)_;
        bindData->paramTypes.push_back(combinedType.copy());
    }
    return bindData;
}

function_set ArrayValueFunction::getFunctionSet() {
    function_set result;
    auto function =
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::ANY},
            LogicalTypeID::ARRAY, ListCreationFunction::execFunc, nullptr, bindFunc);
    function->isVarLength = true;
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
