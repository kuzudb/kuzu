#include "function/list/vector_list_operations.h"
#include "function/map/operations/map_creation_operation.h"
#include "function/map/operations/map_extract_operation.h"
#include "function/map/operations/map_keys_operation.h"
#include "function/map/operations/map_values_operation.h"
#include "function/map/vector_map_operations.h"

namespace kuzu {
namespace function {

vector_operation_definitions MapCreationVectorOperations::getDefinitions() {
    auto execFunc = VectorListOperations::BinaryExecListStructFunction<common::list_entry_t,
        common::list_entry_t, common::list_entry_t, operation::MapCreation>;
    vector_operation_definitions definitions;
    definitions.push_back(make_unique<VectorOperationDefinition>(common::MAP_CREATION_FUNC_NAME,
        std::vector<common::LogicalTypeID>{
            common::LogicalTypeID::VAR_LIST, common::LogicalTypeID::VAR_LIST},
        common::LogicalTypeID::MAP, execFunc, nullptr, bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> MapCreationVectorOperations::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::FunctionDefinition* definition) {
    auto keyType = common::VarListType::getChildType(&arguments[0]->dataType);
    auto valueType = common::VarListType::getChildType(&arguments[1]->dataType);
    std::vector<std::unique_ptr<common::StructField>> structFields;
    structFields.emplace_back(std::make_unique<common::StructField>(
        "key", std::make_unique<common::LogicalType>(*keyType)));
    structFields.emplace_back(std::make_unique<common::StructField>(
        "value", std::make_unique<common::LogicalType>(*valueType)));
    auto mapStructType = std::make_unique<common::LogicalType>(common::LogicalTypeID::STRUCT,
        std::make_unique<common::StructTypeInfo>(std::move(structFields)));
    auto resultType = common::LogicalType(common::LogicalTypeID::MAP,
        std::make_unique<common::VarListTypeInfo>(std::move(mapStructType)));
    return std::make_unique<FunctionBindData>(resultType);
}

vector_operation_definitions MapExtractVectorOperations::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.push_back(make_unique<VectorOperationDefinition>(common::MAP_EXTRACT_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::MAP, common::LogicalTypeID::ANY},
        common::LogicalTypeID::VAR_LIST, nullptr, nullptr, bindFunc, false /* isVarLength */));
    return definitions;
}

static void validateKeyType(std::shared_ptr<binder::Expression> mapExpression,
    std::shared_ptr<binder::Expression> extractKeyExpression) {
    auto mapKeyType = common::MapType::getKeyType(&mapExpression->dataType);
    if (*mapKeyType != extractKeyExpression->dataType) {
        throw common::RuntimeException("Unmatched map key type and extract key type");
    }
}

std::unique_ptr<FunctionBindData> MapExtractVectorOperations::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::FunctionDefinition* definition) {
    validateKeyType(arguments[0], arguments[1]);
    auto vectorOperationDefinition = reinterpret_cast<VectorOperationDefinition*>(definition);
    vectorOperationDefinition->execFunc =
        VectorListOperations::getBinaryListExecFunc<operation::MapExtract, common::list_entry_t>(
            arguments[1]->getDataType());
    auto returnListInfo =
        std::make_unique<common::VarListTypeInfo>(std::make_unique<common::LogicalType>(
            *common::MapType::getValueType(&arguments[0]->dataType)));
    return std::make_unique<FunctionBindData>(
        common::LogicalType(common::LogicalTypeID::VAR_LIST, std::move(returnListInfo)));
}

vector_operation_definitions MapKeysVectorOperations::getDefinitions() {
    auto execFunc = VectorListOperations::UnaryExecListStructFunction<common::list_entry_t,
        common::list_entry_t, operation::MapKeys>;
    vector_operation_definitions definitions;
    definitions.push_back(make_unique<VectorOperationDefinition>(common::MAP_KEYS_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::MAP},
        common::LogicalTypeID::VAR_LIST, execFunc, nullptr, bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> MapKeysVectorOperations::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::FunctionDefinition* definition) {
    auto returnListInfo =
        std::make_unique<common::VarListTypeInfo>(std::make_unique<common::LogicalType>(
            *common::MapType::getKeyType(&arguments[0]->dataType)));
    return std::make_unique<FunctionBindData>(
        common::LogicalType(common::LogicalTypeID::VAR_LIST, std::move(returnListInfo)));
}

vector_operation_definitions MapValuesVectorOperations::getDefinitions() {
    auto execFunc = VectorListOperations::UnaryExecListStructFunction<common::list_entry_t,
        common::list_entry_t, operation::MapValues>;
    vector_operation_definitions definitions;
    definitions.push_back(make_unique<VectorOperationDefinition>(common::MAP_VALUES_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::MAP},
        common::LogicalTypeID::VAR_LIST, execFunc, nullptr, bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> MapValuesVectorOperations::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::FunctionDefinition* definition) {
    auto returnListInfo =
        std::make_unique<common::VarListTypeInfo>(std::make_unique<common::LogicalType>(
            *common::MapType::getValueType(&arguments[0]->dataType)));
    return std::make_unique<FunctionBindData>(
        common::LogicalType(common::LogicalTypeID::VAR_LIST, std::move(returnListInfo)));
}

} // namespace function
} // namespace kuzu
