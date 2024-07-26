#include "function/map/functions/map_keys_function.h"

#include "function/map/vector_map_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(ScalarBindFuncInput input) {
    auto resultType = LogicalType::LIST(MapType::getKeyType(input.arguments[0]->dataType).copy());
    return FunctionBindData::getSimpleBindData(input.arguments, std::move(resultType));
}

function_set MapKeysFunctions::getFunctionSet() {
    auto execFunc =
        ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, list_entry_t, MapKeys>;
    function_set functionSet;
    functionSet.push_back(
        make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::MAP},
            LogicalTypeID::LIST, execFunc, nullptr, bindFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
