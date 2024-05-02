#include "function/map/functions/map_creation_function.h"

#include "function/map/vector_map_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* /*function*/) {
    auto keyType = ListType::getChildType(arguments[0]->dataType);
    auto valueType = ListType::getChildType(arguments[1]->dataType);
    auto resultType = LogicalType::MAP(keyType, valueType);
    return FunctionBindData::getSimpleBindData(arguments, *resultType);
}

function_set MapCreationFunctions::getFunctionSet() {
    auto execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t,
        list_entry_t, MapCreation>;
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::LIST}, LogicalTypeID::MAP,
        execFunc, nullptr, bindFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
