#pragma once

#include "common/vector/value_vector.h"
#include "function/cast/cast_function_bind_data.h"

namespace kuzu {
namespace function {

/**
 * Unary operator assumes operation with null returns null. This does NOT applies to IS_NULL and
 * IS_NOT_NULL operation.
 */

struct UnaryFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static inline void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* /*dataPtr*/) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos),
            resultVector_.getValue<RESULT_TYPE>(resultPos));
    }
};

struct UnarySequenceFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static inline void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* dataPtr) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos),
            resultVector_.getValue<RESULT_TYPE>(resultPos), dataPtr);
    }
};

struct UnaryStringFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* /*dataPtr*/) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos),
            resultVector_.getValue<RESULT_TYPE>(resultPos), resultVector_);
    }
};

struct UnaryCastStringFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* dataPtr) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto resultVector_ = (common::ValueVector*)resultVector;
        // TODO(Ziyi): the reinterpret_cast is not safe since we don't always pass
        // CastFunctionBindData
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos),
            resultVector_->getValue<RESULT_TYPE>(resultPos), resultVector_, inputPos,
            &reinterpret_cast<CastFunctionBindData*>(dataPtr)->option);
    }
};

struct UnaryNestedTypeFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static inline void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* /*dataPtr*/) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos),
            resultVector_.getValue<RESULT_TYPE>(resultPos), inputVector_, resultVector_);
    }
};

struct UnaryCastFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* /*dataPtr*/) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos),
            resultVector_.getValue<RESULT_TYPE>(resultPos), inputVector_, resultVector_);
    }
};

struct UnaryRdfVariantCastFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* /*dataPtr*/) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::template operation<OPERAND_TYPE, RESULT_TYPE>(
            inputVector_.getValue<OPERAND_TYPE>(inputPos), inputVector_, inputPos,
            resultVector_.getValue<RESULT_TYPE>(resultPos), resultVector_, resultPos);
    }
};

struct UnaryUDFFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static inline void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* dataPtr) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos),
            resultVector_.getValue<RESULT_TYPE>(resultPos), dataPtr);
    }
};

struct UnaryFunctionExecutor {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC, typename OP_WRAPPER>
    static void executeOnValue(common::ValueVector& inputVector, uint64_t inputPos,
        common::ValueVector& resultVector, uint64_t resultPos, void* dataPtr) {
        OP_WRAPPER::template operation<OPERAND_TYPE, RESULT_TYPE, FUNC>((void*)&inputVector,
            inputPos, (void*)&resultVector, resultPos, dataPtr);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC, typename OP_WRAPPER>
    static void executeSwitch(common::ValueVector& operand, common::ValueVector& result,
        void* dataPtr) {
        result.resetAuxiliaryBuffer();
        auto& operandSelVector = operand.state->getSelVector();
        if (operand.state->isFlat()) {
            auto inputPos = operandSelVector[0];
            auto resultPos = result.state->getSelVector()[0];
            result.setNull(resultPos, operand.isNull(inputPos));
            if (!result.isNull(resultPos)) {
                executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(operand, inputPos,
                    result, resultPos, dataPtr);
            }
        } else {
            if (operand.hasNoNullsGuarantee()) {
                if (operandSelVector.isUnfiltered()) {
                    for (auto i = 0u; i < operandSelVector.getSelSize(); i++) {
                        executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(operand, i,
                            result, i, dataPtr);
                    }
                } else {
                    for (auto i = 0u; i < operandSelVector.getSelSize(); i++) {
                        auto pos = operandSelVector[i];
                        executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(operand, pos,
                            result, pos, dataPtr);
                    }
                }
            } else {
                if (operandSelVector.isUnfiltered()) {
                    for (auto i = 0u; i < operandSelVector.getSelSize(); i++) {
                        result.setNull(i, operand.isNull(i));
                        if (!result.isNull(i)) {
                            executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(operand, i,
                                result, i, dataPtr);
                        }
                    }
                } else {
                    for (auto i = 0u; i < operandSelVector.getSelSize(); i++) {
                        auto pos = operandSelVector[i];
                        result.setNull(pos, operand.isNull(pos));
                        if (!result.isNull(pos)) {
                            executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(operand,
                                pos, result, pos, dataPtr);
                        }
                    }
                }
            }
        }
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void execute(common::ValueVector& operand, common::ValueVector& result) {
        executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryFunctionWrapper>(operand, result,
            nullptr /* dataPtr */);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeUDF(common::ValueVector& operand, common::ValueVector& result,
        void* dataPtr) {
        executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryUDFFunctionWrapper>(operand, result,
            dataPtr);
    }
};

} // namespace function
} // namespace kuzu
