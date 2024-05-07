#include "function/scalar_function.h"
#include "function/struct/vector_struct_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* /*function*/) {
    std::vector<StructField> fields;
    for (auto& argument : arguments) {
        if (argument->getDataType().getLogicalTypeID() == LogicalTypeID::ANY) {
            argument->cast(*LogicalType::STRING());
        }
        fields.emplace_back(argument->getAlias(), argument->getDataType().copy());
    }
    auto resultType = LogicalType::STRUCT(std::move(fields));
    return FunctionBindData::getSimpleBindData(arguments, *resultType);
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

static void copyParameterValueToStructFieldVector(const ValueVector* parameter,
    ValueVector* structField, DataChunkState* structVectorState) {
    // If the parameter is unFlat, then its state must be consistent with the result's state.
    // Thus, we don't need to copy values to structFieldVector.
    KU_ASSERT(parameter->state->isFlat());
    auto paramPos = parameter->state->getSelVector()[0];
    if (structVectorState->isFlat()) {
        auto pos = structVectorState->getSelVector()[0];
        structField->copyFromVectorData(pos, parameter, paramPos);
    } else {
        for (auto i = 0u; i < structVectorState->getSelVector().getSelSize(); i++) {
            auto pos = structVectorState->getSelVector()[i];
            structField->copyFromVectorData(pos, parameter, paramPos);
        }
    }
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
        StructVector::getFieldVector(&result, i)->resetAuxiliaryBuffer();
        copyParameterValueToStructFieldVector(parameter.get(),
            StructVector::getFieldVector(&result, i).get(), result.state.get());
    }
}

function_set StructPackFunctions::getFunctionSet() {
    function_set functions;
    auto function =
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::ANY},
            LogicalTypeID::STRUCT, execFunc, nullptr, compileFunc, bindFunc);
    function->isVarLength = true;
    functions.push_back(std::move(function));
    return functions;
}

} // namespace function
} // namespace kuzu
