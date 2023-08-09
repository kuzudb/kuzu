#include "function/struct/vector_struct_functions.h"

#include "binder/expression/literal_expression.h"
#include "binder/expression_binder.h"
#include "common/string_utils.h"
#include "function/function_definition.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

vector_function_definitions StructPackVectorFunctions::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(STRUCT_PACK_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY}, LogicalTypeID::STRUCT, execFunc, nullptr,
        compileFunc, bindFunc, true /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> StructPackVectorFunctions::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    std::vector<std::unique_ptr<StructField>> fields;
    for (auto& argument : arguments) {
        if (argument->getDataType().getLogicalTypeID() == LogicalTypeID::ANY) {
            binder::ExpressionBinder::resolveAnyDataType(
                *argument, LogicalType{LogicalTypeID::INT64});
        }
        fields.emplace_back(
            std::make_unique<StructField>(argument->getAlias(), argument->getDataType().copy()));
    }
    auto resultType =
        LogicalType(LogicalTypeID::STRUCT, std::make_unique<StructTypeInfo>(std::move(fields)));
    return std::make_unique<FunctionBindData>(resultType);
}

void StructPackVectorFunctions::execFunc(
    const std::vector<std::shared_ptr<ValueVector>>& parameters, ValueVector& result) {
    for (auto i = 0u; i < parameters.size(); i++) {
        auto& parameter = parameters[i];
        if (parameter->state == result.state) {
            continue;
        }
        // If the parameter's state is inconsistent with the result's state, we need to copy the
        // parameter's value to the corresponding child vector.
        copyParameterValueToStructFieldVector(
            parameter.get(), StructVector::getFieldVector(&result, i).get(), result.state.get());
    }
}

void StructPackVectorFunctions::compileFunc(FunctionBindData* bindData,
    const std::vector<std::shared_ptr<ValueVector>>& parameters,
    std::shared_ptr<ValueVector>& result) {
    // Our goal is to make the state of the resultVector consistent with its children vectors.
    // If the resultVector and inputVector are in different dataChunks, we should create a new
    // child valueVector, which shares the state with the resultVector, instead of reusing the
    // inputVector.
    for (auto i = 0u; i < parameters.size(); i++) {
        if (parameters[i]->state == result->state) {
            StructVector::referenceVector(result.get(), i, parameters[i]);
        }
    }
}

void StructPackVectorFunctions::copyParameterValueToStructFieldVector(
    const ValueVector* parameter, ValueVector* structField, DataChunkState* structVectorState) {
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
    auto inputTypeIDs =
        std::vector<LogicalTypeID>{LogicalTypeID::STRUCT, LogicalTypeID::NODE, LogicalTypeID::REL};
    for (auto inputTypeID : inputTypeIDs) {
        definitions.push_back(make_unique<VectorFunctionDefinition>(STRUCT_EXTRACT_FUNC_NAME,
            std::vector<LogicalTypeID>{inputTypeID, LogicalTypeID::STRING}, LogicalTypeID::ANY,
            nullptr, nullptr, compileFunc, bindFunc, false /* isVarLength */));
    }
    return definitions;
}

std::unique_ptr<FunctionBindData> StructExtractVectorFunctions::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto structType = arguments[0]->getDataType();
    if (arguments[1]->expressionType != LITERAL) {
        throw BinderException("Key name for struct/union extract must be STRING literal.");
    }
    auto key = ((binder::LiteralExpression&)*arguments[1]).getValue()->getValue<std::string>();
    assert(definition->returnTypeID == LogicalTypeID::ANY);
    auto fieldIdx = StructType::getFieldIdx(&structType, key);
    if (fieldIdx == INVALID_STRUCT_FIELD_IDX) {
        throw BinderException(StringUtils::string_format("Invalid struct field name: {}.", key));
    }
    return std::make_unique<StructExtractBindData>(
        *(StructType::getFieldTypes(&structType))[fieldIdx], fieldIdx);
}

void StructExtractVectorFunctions::compileFunc(FunctionBindData* bindData,
    const std::vector<std::shared_ptr<ValueVector>>& parameters,
    std::shared_ptr<ValueVector>& result) {
    assert(parameters[0]->dataType.getPhysicalType() == PhysicalTypeID::STRUCT);
    auto structBindData = reinterpret_cast<StructExtractBindData*>(bindData);
    result = StructVector::getFieldVector(parameters[0].get(), structBindData->childIdx);
    result->state = parameters[0]->state;
}

} // namespace function
} // namespace kuzu
