#include "function/map/functions/map_values_function.h"

#include "function/map/vector_map_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* /*function*/) {
    auto resultType = LogicalType::LIST(MapType::getValueType(&arguments[0]->dataType)->copy());
    return FunctionBindData::getSimpleBindData(arguments, *resultType);
}

function_set MapValuesFunctions::getFunctionSet() {
    auto execFunc =
        ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, list_entry_t, MapValues>;
    function_set functionSet;
    functionSet.push_back(
        make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::MAP},
            LogicalTypeID::LIST, execFunc, nullptr, bindFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
