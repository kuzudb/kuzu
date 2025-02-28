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
        uint64_t /* resultPos */, void* dataPtr) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos), resultVector_, dataPtr);
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

struct UnaryStructFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void operation(void* /*inputVector*/, uint64_t /*inputPos*/, void* resultVector,
        uint64_t resultPos, void* dataPtr) {
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::operation(resultVector_.getValue<RESULT_TYPE>(resultPos), resultVector_, dataPtr);
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

    template<bool operandIsFiltered, bool resultIsFiltered>
    static std::pair<common::sel_t, common::sel_t> getSelectedPos(common::idx_t selIdx,
        [[maybe_unused]] common::SelectionVector* operandSelVector,
        [[maybe_unused]] common::SelectionVector* resultSelVector) {
        common::sel_t operandPos{}, resultPos{};
        if constexpr (operandIsFiltered) {
            operandPos = (*operandSelVector)[selIdx];
        } else {
            operandPos = selIdx;
        }
        if constexpr (resultIsFiltered) {
            resultPos = (*resultSelVector)[selIdx];
        } else {
            resultPos = selIdx;
        }
        return {operandPos, resultPos};
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC, typename OP_WRAPPER,
        bool noNullsGuaranteed, bool operandIsFiltered, bool resultIsFiltered>
    static void executeOnValueWrapper(common::ValueVector& operand,
        common::SelectionVector* operandSelVector, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr) {
        for (auto i = 0u; i < operandSelVector->getSelSize(); i++) {
            const auto [operandPos, resultPos] =
                getSelectedPos<operandIsFiltered, resultIsFiltered>(i, operandSelVector,
                    resultSelVector);
            if constexpr (noNullsGuaranteed) {
                executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(operand, operandPos,
                    result, resultPos, dataPtr);
            } else {
                result.setNull(resultPos, operand.isNull(operandPos));
                if (!result.isNull(resultPos)) {
                    executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(operand, operandPos,
                        result, resultPos, dataPtr);
                }
            }
        }
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC, typename OP_WRAPPER>
    static void executeSwitch(common::ValueVector& operand,
        common::SelectionVector* operandSelVector, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr) {
        result.resetAuxiliaryBuffer();
        if (operand.state->isFlat()) {
            auto inputPos = (*operandSelVector)[0];
            auto resultPos = (*resultSelVector)[0];
            result.setNull(resultPos, operand.isNull(inputPos));
            if (!result.isNull(resultPos)) {
                executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(operand, inputPos,
                    result, resultPos, dataPtr);
            }
        } else {
            if (operand.hasNoNullsGuarantee()) {
                result.setAllNonNull();
                if (operandSelVector->isUnfiltered()) {
                    if (resultSelVector->isUnfiltered()) {
                        executeOnValueWrapper<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER, true,
                            false, false>(operand, operandSelVector, result, resultSelVector,
                            dataPtr);
                    } else {
                        executeOnValueWrapper<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER, true,
                            false, true>(operand, operandSelVector, result, resultSelVector,
                            dataPtr);
                    }
                } else {
                    if (resultSelVector->isUnfiltered()) {
                        executeOnValueWrapper<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER, true,
                            true, false>(operand, operandSelVector, result, resultSelVector,
                            dataPtr);
                    } else {
                        executeOnValueWrapper<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER, true,
                            true, true>(operand, operandSelVector, result, resultSelVector,
                            dataPtr);
                    }
                }
            } else {
                if (operandSelVector->isUnfiltered()) {
                    if (resultSelVector->isUnfiltered()) {
                        executeOnValueWrapper<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER, false,
                            false, false>(operand, operandSelVector, result, resultSelVector,
                            dataPtr);
                    } else {
                        executeOnValueWrapper<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER, false,
                            false, true>(operand, operandSelVector, result, resultSelVector,
                            dataPtr);
                    }
                } else {
                    if (resultSelVector->isUnfiltered()) {
                        executeOnValueWrapper<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER, false,
                            true, false>(operand, operandSelVector, result, resultSelVector,
                            dataPtr);
                    } else {
                        executeOnValueWrapper<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER, false,
                            true, true>(operand, operandSelVector, result, resultSelVector,
                            dataPtr);
                    }
                }
            }
        }
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void execute(common::ValueVector& operand, common::SelectionVector* operandSelVector,
        common::ValueVector& result, common::SelectionVector* resultSelVector) {
        executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryFunctionWrapper>(operand,
            operandSelVector, result, resultSelVector, nullptr /* dataPtr */);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeSequence(common::ValueVector& operand,
        common::SelectionVector* operandSelVector, common::ValueVector& result,
        common::SelectionVector* resultSelVector, void* dataPtr) {
        result.resetAuxiliaryBuffer();
        auto inputPos = (*operandSelVector)[0];
        auto resultPos = (*resultSelVector)[0];
        executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, UnarySequenceFunctionWrapper>(operand,
            inputPos, result, resultPos, dataPtr);
    }
};

} // namespace function
} // namespace kuzu
