#include "function/struct/vector_struct_functions.h"

#include "binder/expression/literal_expression.h"
#include "binder/expression_binder.h"
#include "common/string_utils.h"
#include "function/function_definition.h"

namespace kuzu {
namespace function {

vector_function_definitions StructPackVectorFunctions::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(common::STRUCT_PACK_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::ANY},
        common::LogicalTypeID::STRUCT, execFunc, nullptr, compileFunc, bindFunc,
        true /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> StructPackVectorFunctions::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    std::vector<std::unique_ptr<common::StructField>> fields;
    for (auto& argument : arguments) {
        if (argument->getDataType().getLogicalTypeID() == common::LogicalTypeID::ANY) {
            binder::ExpressionBinder::resolveAnyDataType(
                *argument, common::LogicalType{common::LogicalTypeID::INT64});
        }
        fields.emplace_back(std::make_unique<common::StructField>(
            argument->getAlias(), argument->getDataType().copy()));
    }
    auto resultType = common::LogicalType(
        common::LogicalTypeID::STRUCT, std::make_unique<common::StructTypeInfo>(std::move(fields)));
    return std::make_unique<FunctionBindData>(resultType);
}

void StructPackVectorFunctions::execFunc(
    const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    common::ValueVector& result) {
    for (auto i = 0u; i < parameters.size(); i++) {
        auto& parameter = parameters[i];
        if (parameter->state == result.state) {
            continue;
        }
        // If the parameter's state is inconsistent with the result's state, we need to copy the
        // parameter's value to the corresponding child vector.
        copyParameterValueToStructFieldVector(parameter.get(),
            common::StructVector::getFieldVector(&result, i).get(), result.state.get());
    }
}

void StructPackVectorFunctions::compileFunc(FunctionBindData* bindData,
    const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    std::shared_ptr<common::ValueVector>& result) {
    // Our goal is to make the state of the resultVector consistent with its children vectors.
    // If the resultVector and inputVector are in different dataChunks, we should create a new
    // child valueVector, which shares the state with the resultVector, instead of reusing the
    // inputVector.
    for (auto i = 0u; i < parameters.size(); i++) {
        if (parameters[i]->state == result->state) {
            common::StructVector::referenceVector(result.get(), i, parameters[i]);
        }
    }
}

void StructPackVectorFunctions::copyParameterValueToStructFieldVector(
    const common::ValueVector* parameter, common::ValueVector* structField,
    common::DataChunkState* structVectorState) {
    // If the parameter is unFlat, then its state must be consistent with the result's state.
    // Thus, we don't need to copy values to structFieldVector.
    assert(parameter->state->isFlat());
    auto paramPos = parameter->state->selVector->selectedPositions[0];
    if (structVectorState->isFlat()) {
        auto pos = structVectorState->selVector->selectedPositions[0];
        structField->copyFromVectorData(pos, parameter, paramPos);
    } else {
        for (auto i = 0u; i < structVectorState->selVector->selectedSize; i++) {
            auto pos = structVectorState->selVector->selectedPositions[i];
            structField->copyFromVectorData(pos, parameter, paramPos);
        }
    }
}

vector_function_definitions StructExtractVectorFunctions::getDefinitions() {
    vector_function_definitions definitions;
    auto inputTypeIDs = std::vector<common::LogicalTypeID>{
        common::LogicalTypeID::STRUCT, common::LogicalTypeID::NODE, common::LogicalTypeID::REL};
    for (auto inputTypeID : inputTypeIDs) {
        definitions.push_back(
            make_unique<VectorFunctionDefinition>(common::STRUCT_EXTRACT_FUNC_NAME,
                std::vector<common::LogicalTypeID>{inputTypeID, common::LogicalTypeID::STRING},
                common::LogicalTypeID::ANY, nullptr, nullptr, compileFunc, bindFunc,
                false /* isVarLength */));
    }
    return definitions;
}

std::unique_ptr<FunctionBindData> StructExtractVectorFunctions::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto structType = arguments[0]->getDataType();
    if (arguments[1]->expressionType != common::LITERAL) {
        throw common::BinderException("Key name for struct/union extract must be STRING literal.");
    }
    auto key = ((binder::LiteralExpression&)*arguments[1]).getValue()->getValue<std::string>();
    assert(definition->returnTypeID == common::LogicalTypeID::ANY);
    auto fieldIdx = common::StructType::getFieldIdx(&structType, key);
    if (fieldIdx == common::INVALID_STRUCT_FIELD_IDX) {
        throw common::BinderException(
            common::StringUtils::string_format("Invalid struct field name: {}.", key));
    }
    return std::make_unique<StructExtractBindData>(
        *(common::StructType::getFieldTypes(&structType))[fieldIdx], fieldIdx);
}

void StructExtractVectorFunctions::compileFunc(FunctionBindData* bindData,
    const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    std::shared_ptr<common::ValueVector>& result) {
    assert(parameters[0]->dataType.getPhysicalType() == common::PhysicalTypeID::STRUCT);
    auto structBindData = reinterpret_cast<StructExtractBindData*>(bindData);
    result = common::StructVector::getFieldVector(parameters[0].get(), structBindData->childIdx);
    result->state = parameters[0]->state;
}

} // namespace function
} // namespace kuzu
