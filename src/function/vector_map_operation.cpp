#include "function/list/vector_list_operations.h"
#include "function/map/operations/map_operation.h"
#include "function/map/vector_map_operations.h"

namespace kuzu {
namespace function {

vector_operation_definitions MapVectorOperations::getDefinitions() {
    vector_operation_definitions definitions;
    auto excFunct = VectorListOperations::BinaryListExecFunction<common::list_entry_t,
        common::list_entry_t, common::list_entry_t, operation::Map>;
    definitions.push_back(make_unique<VectorOperationDefinition>(common::MAP_CREATION_FUNC_NAME,
        std::vector<common::LogicalTypeID>{
            common::LogicalTypeID::VAR_LIST, common::LogicalTypeID::VAR_LIST},
        common::LogicalTypeID::MAP, excFunct, nullptr, bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> MapVectorOperations::bindFunc(
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

} // namespace function
} // namespace kuzu
