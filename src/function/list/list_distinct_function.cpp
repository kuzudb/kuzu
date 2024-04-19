#include "function/list/functions/list_distinct_function.h"

#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function*) {
    return FunctionBindData::getSimpleBindData(arguments, arguments[0]->getDataType());
}

function_set ListDistinctFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST}, LogicalTypeID::LIST,
        ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, list_entry_t, ListDistinct>,
        nullptr, bindFunc));
    return result;
}

} // namespace function
} // namespace kuzu
