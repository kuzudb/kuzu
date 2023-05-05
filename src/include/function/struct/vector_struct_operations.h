#pragma once

#include "common/vector/value_vector_utils.h"
#include "function/vector_operations.h"

namespace kuzu {
namespace function {

struct StructPackVectorOperations : public VectorOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result) {
        for (auto i = 0u; i < parameters.size(); i++) {
            auto& parameter = parameters[i];
            if (parameter->state == result.state) {
                continue;
            }
            // If the parameter's state is inconsistent with the result's state, we need to copy the
            // parameter's value to the corresponding child vector.
            copyParameterValueToStructFieldVector(
                parameter.get(), common::StructVector::getChildVector(&result, i).get());
        }
    }
    static void copyParameterValueToStructFieldVector(
        const common::ValueVector* parameter, common::ValueVector* structField) {
        // If the parameter is unFlat, then its state must be consistent with the result's state.
        // Thus, we don't need to copy values to structFieldVector.
        assert(parameter->state->isFlat());
        auto srcValue =
            parameter->getData() +
            parameter->getNumBytesPerValue() * parameter->state->selVector->selectedPositions[0];
        if (structField->state->isFlat()) {
            common::ValueVectorUtils::copyValue(
                structField->getData() + structField->getNumBytesPerValue() *
                                             structField->state->selVector->selectedPositions[0],
                *structField, srcValue, *parameter);
        } else {
            for (auto j = 0u; j < structField->state->selVector->selectedSize; j++) {
                auto pos = structField->state->selVector->selectedPositions[j];
                common::ValueVectorUtils::copyValue(
                    structField->getData() + structField->getNumBytesPerValue() * pos, *structField,
                    srcValue, *parameter);
            }
        }
    }
};

struct StructExtractBindData : public FunctionBindData {
    common::vector_idx_t childIdx;

    StructExtractBindData(common::DataType dataType, common::vector_idx_t childIdx)
        : FunctionBindData{std::move(dataType)}, childIdx{childIdx} {}
};

struct StructExtractVectorOperations : public VectorOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result) {} // Evaluate at compile time
};

} // namespace function
} // namespace kuzu
