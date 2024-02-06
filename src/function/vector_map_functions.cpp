#include "function/map/vector_map_functions.h"

#include "function/list/vector_list_functions.h"
#include "function/map/functions/map_creation_function.h"
#include "function/map/functions/map_extract_function.h"
#include "function/map/functions/map_keys_function.h"
#include "function/map/functions/map_values_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

function_set MapCreationFunctions::getFunctionSet() {
    auto execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t,
        list_entry_t, MapCreation>;
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(MAP_CREATION_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::VAR_LIST},
        LogicalTypeID::MAP, execFunc, nullptr, bindFunc, false /* isVarLength */));
    return functionSet;
}

std::unique_ptr<FunctionBindData> MapCreationFunctions::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::Function* /*function*/) {
    auto keyType = VarListType::getChildType(&arguments[0]->dataType);
    auto valueType = VarListType::getChildType(&arguments[1]->dataType);
    auto resultType = LogicalType::MAP(*keyType, *valueType);
    return std::make_unique<FunctionBindData>(std::move(resultType));
}

function_set MapExtractFunctions::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(MAP_EXTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::MAP, LogicalTypeID::ANY}, LogicalTypeID::VAR_LIST,
        nullptr, nullptr, bindFunc, false /* isVarLength */));
    return functionSet;
}

static void validateKeyType(const std::shared_ptr<binder::Expression>& mapExpression,
    const std::shared_ptr<binder::Expression>& extractKeyExpression) {
    auto mapKeyType = MapType::getKeyType(&mapExpression->dataType);
    if (*mapKeyType != extractKeyExpression->dataType) {
        throw RuntimeException("Unmatched map key type and extract key type");
    }
}

std::unique_ptr<FunctionBindData> MapExtractFunctions::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::Function* function) {
    validateKeyType(arguments[0], arguments[1]);
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    scalarFunction->execFunc =
        ListFunction::getBinaryListExecFuncSwitchRight<MapExtract, list_entry_t>(
            arguments[1]->getDataType());
    return std::make_unique<FunctionBindData>(LogicalType::VAR_LIST(
        std::make_unique<LogicalType>(*MapType::getValueType(&arguments[0]->dataType))));
}

function_set MapKeysFunctions::getFunctionSet() {
    auto execFunc =
        ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, list_entry_t, MapKeys>;
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(MAP_KEYS_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::MAP}, LogicalTypeID::VAR_LIST, execFunc, nullptr,
        bindFunc, false /* isVarLength */));
    return functionSet;
}

std::unique_ptr<FunctionBindData> MapKeysFunctions::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::Function* /*function*/) {
    return std::make_unique<FunctionBindData>(LogicalType::VAR_LIST(
        std::make_unique<LogicalType>(*MapType::getKeyType(&arguments[0]->dataType))));
}

function_set MapValuesFunctions::getFunctionSet() {
    auto execFunc =
        ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, list_entry_t, MapValues>;
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(MAP_VALUES_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::MAP}, LogicalTypeID::VAR_LIST, execFunc, nullptr,
        bindFunc, false /* isVarLength */));
    return functionSet;
}

std::unique_ptr<FunctionBindData> MapValuesFunctions::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::Function* /*function*/) {
    return std::make_unique<FunctionBindData>(LogicalType::VAR_LIST(
        std::make_unique<LogicalType>(*MapType::getValueType(&arguments[0]->dataType))));
}

} // namespace function
} // namespace kuzu
