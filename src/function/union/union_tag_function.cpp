#include "function/scalar_function.h"
#include "function/union/functions/union_tag.h"
#include "function/union/vector_union_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(ScalarBindFuncInput input) {
    return FunctionBindData::getSimpleBindData(input.arguments, LogicalType::STRING());
}

function_set UnionTagFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::UNION}, LogicalTypeID::STRING,
        ScalarFunction::UnaryExecNestedTypeFunction<union_entry_t, ku_string_t, UnionTag>, nullptr,
        bindFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
