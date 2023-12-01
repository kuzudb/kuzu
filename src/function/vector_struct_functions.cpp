#include "function/struct/vector_struct_functions.h"

#include "binder/expression/literal_expression.h"
#include "binder/expression_binder.h"
#include "common/exception/binder.h"
#include "common/string_format.h"
#include "function/function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

function_set StructPackFunctions::getFunctionSet() {
    function_set functions;
    functions.push_back(make_unique<ScalarFunction>(STRUCT_PACK_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY}, LogicalTypeID::STRUCT, execFunc, nullptr,
        compileFunc, bindFunc, true /* isVarLength */));
    return functions;
}

std::unique_ptr<FunctionBindData> StructPackFunctions::bindFunc(
    const binder::expression_vector& arguments, Function* /*function*/) {
    std::vector<std::unique_ptr<StructField>> fields;
    for (auto& argument : arguments) {
        if (argument->getDataType().getLogicalTypeID() == LogicalTypeID::ANY) {
            binder::ExpressionBinder::resolveAnyDataType(
                *argument, LogicalType{LogicalTypeID::STRING});
        }
        fields.emplace_back(
            std::make_unique<StructField>(argument->getAlias(), argument->getDataType().copy()));
    }
    auto resultType =
        LogicalType(LogicalTypeID::STRUCT, std::make_unique<StructTypeInfo>(std::move(fields)));
    return std::make_unique<FunctionBindData>(resultType);
}

void StructPackFunctions::execFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
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

void StructPackFunctions::compileFunc(FunctionBindData* /*bindData*/,
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

void StructPackFunctions::copyParameterValueToStructFieldVector(
    const ValueVector* parameter, ValueVector* structField, DataChunkState* structVectorState) {
    // If the parameter is unFlat, then its state must be consistent with the result's state.
    // Thus, we don't need to copy values to structFieldVector.
    KU_ASSERT(parameter->state->isFlat());
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

function_set StructExtractFunctions::getFunctionSet() {
    function_set functions;
    auto inputTypeIDs =
        std::vector<LogicalTypeID>{LogicalTypeID::STRUCT, LogicalTypeID::NODE, LogicalTypeID::REL};
    for (auto inputTypeID : inputTypeIDs) {
        functions.push_back(make_unique<ScalarFunction>(STRUCT_EXTRACT_FUNC_NAME,
            std::vector<LogicalTypeID>{inputTypeID, LogicalTypeID::STRING}, LogicalTypeID::ANY,
            nullptr, nullptr, compileFunc, bindFunc, false /* isVarLength */));
    }
    return functions;
}

std::unique_ptr<FunctionBindData> StructExtractFunctions::bindFunc(
    const binder::expression_vector& arguments, Function* /*function*/) {
    auto structType = arguments[0]->getDataType();
    if (arguments[1]->expressionType != ExpressionType::LITERAL) {
        throw BinderException("Key name for struct/union extract must be STRING literal.");
    }
    auto key = ((binder::LiteralExpression&)*arguments[1]).getValue()->getValue<std::string>();
    auto fieldIdx = StructType::getFieldIdx(&structType, key);
    if (fieldIdx == INVALID_STRUCT_FIELD_IDX) {
        throw BinderException(stringFormat("Invalid struct field name: {}.", key));
    }
    return std::make_unique<StructExtractBindData>(
        *(StructType::getFieldTypes(&structType))[fieldIdx], fieldIdx);
}

void StructExtractFunctions::compileFunc(FunctionBindData* bindData,
    const std::vector<std::shared_ptr<ValueVector>>& parameters,
    std::shared_ptr<ValueVector>& result) {
    KU_ASSERT(parameters[0]->dataType.getPhysicalType() == PhysicalTypeID::STRUCT);
    auto structBindData = reinterpret_cast<StructExtractBindData*>(bindData);
    result = StructVector::getFieldVector(parameters[0].get(), structBindData->childIdx);
    result->state = parameters[0]->state;
}

} // namespace function
} // namespace kuzu
