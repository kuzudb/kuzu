#include "function/list/functions/list_reverse_function.h"

#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* function) {
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    auto resultType = arguments[0]->dataType;
    scalarFunction->execFunc =
        ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, list_entry_t, ListReverse>;
    return FunctionBindData::getSimpleBindData(arguments, resultType);
}

function_set ListReverseFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::LIST},
            LogicalTypeID::ANY, nullptr, nullptr, bindFunc));
    return result;
}

} // namespace function
} // namespace kuzu
