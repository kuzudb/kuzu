#include "function/map/vector_map_functions.h"

#include "function/list/vector_list_functions.h"
#include "function/map/functions/map_creation_function.h"
#include "function/map/functions/map_extract_function.h"
#include "function/map/functions/map_keys_function.h"
#include "function/map/functions/map_values_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

vector_function_definitions MapCreationVectorFunctions::getDefinitions() {
    auto execFunc = VectorListFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t,
        list_entry_t, MapCreation>;
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(MAP_CREATION_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::VAR_LIST},
        LogicalTypeID::MAP, execFunc, nullptr, bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> MapCreationVectorFunctions::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::FunctionDefinition* definition) {
    auto keyType = VarListType::getChildType(&arguments[0]->dataType);
    auto valueType = VarListType::getChildType(&arguments[1]->dataType);
    std::vector<std::unique_ptr<StructField>> structFields;
    structFields.emplace_back(std::make_unique<StructField>(
        InternalKeyword::MAP_KEY, std::make_unique<LogicalType>(*keyType)));
    structFields.emplace_back(std::make_unique<StructField>(
        InternalKeyword::MAP_VALUE, std::make_unique<LogicalType>(*valueType)));
    auto mapStructType = std::make_unique<LogicalType>(
        LogicalTypeID::STRUCT, std::make_unique<StructTypeInfo>(std::move(structFields)));
    auto resultType = LogicalType(
        LogicalTypeID::MAP, std::make_unique<VarListTypeInfo>(std::move(mapStructType)));
    return std::make_unique<FunctionBindData>(resultType);
}

vector_function_definitions MapExtractVectorFunctions::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(MAP_EXTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::MAP, LogicalTypeID::ANY}, LogicalTypeID::VAR_LIST,
        nullptr, nullptr, bindFunc, false /* isVarLength */));
    return definitions;
}

static void validateKeyType(std::shared_ptr<binder::Expression> mapExpression,
    std::shared_ptr<binder::Expression> extractKeyExpression) {
    auto mapKeyType = MapType::getKeyType(&mapExpression->dataType);
    if (*mapKeyType != extractKeyExpression->dataType) {
        throw RuntimeException("Unmatched map key type and extract key type");
    }
}

std::unique_ptr<FunctionBindData> MapExtractVectorFunctions::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::FunctionDefinition* definition) {
    validateKeyType(arguments[0], arguments[1]);
    auto vectorFunctionDefinition = reinterpret_cast<VectorFunctionDefinition*>(definition);
    vectorFunctionDefinition->execFunc =
        VectorListFunction::getBinaryListExecFunc<MapExtract, list_entry_t>(
            arguments[1]->getDataType());
    auto returnListInfo = std::make_unique<VarListTypeInfo>(
        std::make_unique<LogicalType>(*MapType::getValueType(&arguments[0]->dataType)));
    return std::make_unique<FunctionBindData>(
        LogicalType(LogicalTypeID::VAR_LIST, std::move(returnListInfo)));
}

vector_function_definitions MapKeysVectorFunctions::getDefinitions() {
    auto execFunc =
        VectorListFunction::UnaryExecListStructFunction<list_entry_t, list_entry_t, MapKeys>;
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(MAP_KEYS_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::MAP}, LogicalTypeID::VAR_LIST, execFunc, nullptr,
        bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> MapKeysVectorFunctions::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::FunctionDefinition* definition) {
    auto returnListInfo = std::make_unique<VarListTypeInfo>(
        std::make_unique<LogicalType>(*MapType::getKeyType(&arguments[0]->dataType)));
    return std::make_unique<FunctionBindData>(
        LogicalType(LogicalTypeID::VAR_LIST, std::move(returnListInfo)));
}

vector_function_definitions MapValuesVectorFunctions::getDefinitions() {
    auto execFunc =
        VectorListFunction::UnaryExecListStructFunction<list_entry_t, list_entry_t, MapValues>;
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(MAP_VALUES_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::MAP}, LogicalTypeID::VAR_LIST, execFunc, nullptr,
        bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> MapValuesVectorFunctions::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::FunctionDefinition* definition) {
    auto returnListInfo = std::make_unique<VarListTypeInfo>(
        std::make_unique<LogicalType>(*MapType::getValueType(&arguments[0]->dataType)));
    return std::make_unique<FunctionBindData>(
        LogicalType(LogicalTypeID::VAR_LIST, std::move(returnListInfo)));
}

} // namespace function
} // namespace kuzu
